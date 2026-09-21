// Request path shared by PBFT and SBFT: client request buffering, the
// request timer, (delayed) proposals, and execution. Every method here runs
// under the engine mutex unless noted.

#include "core/Entity.h"
#include "core/Log.h"
#include "core/events/EventFactory.h"
#include "core/events/ProtoMessage.h"

#include <algorithm>
#include <atomic>
#include <vector>

using json = nlohmann::json;

std::string Entity::requestKey(const std::string& clientId, uint64_t requestId) {
    return clientId + "/" + std::to_string(requestId);
}

std::string Entity::aggregationKey(const std::string& phase, int view, int seq) {
    return phase + "_v" + std::to_string(view) + "_" + std::to_string(seq);
}

std::size_t Entity::senderCount(const std::string& phase, int view, int seq) const {
    std::lock_guard<std::mutex> lk(senderIdsMtx);
    auto it = keyToSenderIds.find(aggregationKey(phase, view, seq));
    return it == keyToSenderIds.end() ? 0 : it->second.size();
}

std::set<int> Entity::senders(const std::string& phase, int view, int seq) const {
    std::lock_guard<std::mutex> lk(senderIdsMtx);
    auto it = keyToSenderIds.find(aggregationKey(phase, view, seq));
    if (it == keyToSenderIds.end()) return {};
    return std::set<int>(it->second.begin(), it->second.end());
}

std::size_t Entity::pendingRequestCount() const {
    return pendingRequests_.size();
}

void Entity::onClientRequest(const json& request) {
    const std::string clientId = request.value("client_id", std::string());
    const uint64_t requestId = request.value("request_id", (uint64_t)0);
    const std::string operation = request.value("operation", std::string());
    if (clientId.empty() || operation.empty()) {
        static std::atomic<uint64_t> malformed{0};
        const uint64_t count = malformed.fetch_add(1) + 1;
        if (count == 1 || count % 1000 == 0) {
            LOG_WARN("client request without client id or operation (" << count << " so far); dropping");
        }
        return;
    }
    const std::string key = requestKey(clientId, requestId);

    if (!pbftCore_) {
        // Protocols without the PBFT core keep the original behaviour: the
        // leader proposes, followers drop.
        if (!isCurrentLeader()) {
            static std::atomic<uint64_t> misrouted{0};
            const uint64_t count = misrouted.fetch_add(1) + 1;
            if (count == 1 || count % 1000 == 0) {
                LOG_WARN("received client request " << key << " while not the leader (leader="
                         << currentLeader() << ", " << count << " so far); dropping");
            }
            return;
        }
        PendingRequest pr;
        pr.request = request;
        pr.clientId = clientId;
        pr.requestId = requestId;
        pr.arrivalUs = request.value("arrival_us", nowUs());
        pr.acceptedAt = Clock::now();
        pendingRequests_[key] = std::move(pr);
        proposeRequest(key);
        return;
    }

    bedrock::ClientRequest wire;
    wire.set_client_id(clientId); wire.set_request_id(requestId); wire.set_operation(operation);
    wire.set_timestamp(request.value("timestamp", std::string()));
    wire.set_signature(request.value("signature", std::string()));
    const auto tx = request.value("transaction", json::object());
    wire.mutable_transaction()->set_from(tx.value("from", std::string()));
    wire.mutable_transaction()->set_to(tx.value("to", std::string()));
    wire.mutable_transaction()->set_amount(tx.value("amount", 0));
    acceptClientRequest(wire, request.value("arrival_us", nowUs()));
}

// Every replica observes the leader a few times per second so the failure
// spec pins the same leader window on all replicas at each phase or
// interval-tick boundary, whether or not this replica ever leads.
void Entity::scheduleLeaderObservation() {
    if (!proposalDelay_->enabled()) return;
    scheduler_.scheduleAfter(std::chrono::milliseconds(200), [this] {
        {
            std::lock_guard<std::recursive_mutex> engine(eventMtx);
            if (!running) return;
            proposalDelay_->observeLeader(currentLeader(), peerIds_);
        }
        scheduleLeaderObservation();
    });
}

void Entity::cancelDelayedProposals() {
    const std::size_t dropped = scheduler_.cancelAll();
    scheduleLeaderObservation();
    scheduleConsensusMaintenance();
    ++proposalGeneration_;
    nextProposalTick_ = Clock::now() + std::chrono::milliseconds(options_.proposalIntervalMs);
    scheduleProposalTick();
    if (dropped > 0) LOG_DEBUG("dropped " << dropped << " scheduled task(s)");
}

void Entity::scheduleProposal(const std::string& key) {
    const auto delay = proposalDelay_->delayForProposal(nodeId, currentLeader(), peerIds_);
    if (delay.count() <= 0) {
        proposeRequest(key);
        return;
    }
    const uint64_t count = delayedProposals_.fetch_add(1) + 1;
    if (count == 1 || count % 500 == 0) {
        LOG_INFO("injecting proposal delay of " << delay.count() << " ms for request " << key
                 << " (" << count << " delayed so far)");
    }
    scheduler_.scheduleAfter(delay, [this, key] {
        std::lock_guard<std::recursive_mutex> engine(eventMtx);
        if (!running) return;
        proposeRequest(key);
    });
}

void Entity::proposeRequest(const std::string& key) {
    auto it = pendingRequests_.find(key);
    if (it == pendingRequests_.end()) return;  // executed meanwhile
    PendingRequest& pr = it->second;
    if (!isCurrentLeader() || inViewChange) return;  // stays buffered for the next leader
    const int view = currentView();
    if (pr.proposedInView == view) return;
    if (pbftCore_ && static_cast<int64_t>(nextSequenceNumber.load()) >=
        static_cast<int64_t>(stableCheckpoint_) + consensusWindow()) return;
    pr.proposedInView = view;

    const int seq = allocateNextSequence();
    noteFirstSeen(seq, pr.arrivalUs);
    if (!sequenceStates.count(seq)) {
        sequenceStates.emplace(seq, EntityState(getState().getRole(), "Request", view, seq));
    }

    const json& j = pr.request;
    const json transaction = j.value("transaction", json::object());
    const std::string timestamp = j.value("timestamp", std::string());
    const std::string signature = !pbftCore_ && options_.proposalSigning
        ? cryptoProvider->sign(transaction.dump() + timestamp)
        : std::string();

    bedrock::ProtocolEnvelope env;
    auto* m = env.mutable_pre_prepare();
    m->set_view(view);
    m->set_sequence(seq);
    m->set_timestamp(timestamp);
    m->set_operation(j.value("operation", std::string()));
    auto* tx = m->mutable_transaction();
    tx->set_from(transaction.value("from", std::string()));
    tx->set_to(transaction.value("to", std::string()));
    tx->set_amount(transaction.value("amount", 0));
    m->set_client_id(pr.clientId);
    m->set_request_id(pr.requestId);
    m->set_signature(signature);
    m->set_message_sender_id(nodeId);
    m->set_type(getPhaseConfig("Request")["next_state"].as<std::string>());

    if (pbftCore_) {
        env.set_digest(bedrock::requestDigest(*m));
        signEnvelope(env);
        handleConsensusEnvelope(env);
        sendProtocolToAll(env);
        return;
    }
    // Legacy protocols store locally through their YAML action.
    ProtoMessage pmsg(env);
    auto storeEv = EventFactory::getInstance().createEvent("storeMessage");
    if (storeEv) storeEv->execute(this, &pmsg, &sequenceStates[seq]);
    LOG_DEBUG("proposing seq " << seq << " for request " << key << " in view " << view);
    sendProtocolToAll(env);
}

void Entity::proposeBufferedRequests() {
    if (pbftCore_) { rebuildProposalQueue(); return; }
    std::vector<std::pair<Clock::time_point, std::string>> order;
    const int view = currentView();
    for (const auto& [key, pr] : pendingRequests_) {
        if (pr.proposedInView == view) continue;
        order.emplace_back(pr.acceptedAt, key);
    }
    std::sort(order.begin(), order.end());
    if (!order.empty()) {
        LOG_INFO("proposing " << order.size() << " buffered requests as leader of view " << view);
    }
    for (const auto& [acceptedAt, key] : order) {
        scheduleProposal(key);
    }
}

void Entity::onPrePrepareAccepted(int seq, int view) {
    (void)view;
    if (!pbftCore_ || inViewChange) return;
    if (hasProcessedOperation(seq)) return;
    requestTimer_.track("seq:" + std::to_string(seq));
}

bool Entity::completeSequence(int seq, int path) {
    if (hasProcessedOperation(seq)) return true;
    if (pbftCore_ && (seq != lastExecuted_ + 1 || !readySequences_.count(seq))) return false;

    if (pbftCore_) {
        auto found = batchIndex_.find(seq);
        if (found == batchIndex_.end()) return false;
        // The executed batch stays in the log until a stable checkpoint
        // prunes it, so this replica can still serve those bytes to one that
        // is catching up. Nothing below erases from batchIndex_.
        const auto& batch = found->second;
        const auto executeStart = nowUs();
        uint32_t executed = 0;
        std::vector<std::string> completed;
        completed.reserve(batch.requests_size() + 1);
        // One reply queue per client and one balance-lock acquisition for the
        // whole batch. Per-request replies cost a condition-variable wake each,
        // which at thousands of requests per batch dominates execution.
        std::unordered_map<std::string, std::vector<bedrock::ClientReply>> replies;
        const int view = currentView();
        const int leader = currentLeader();
        {
            std::lock_guard<std::mutex> balanceLock(balancesMutex);
            for (const auto& r : batch.requests()) {
                const auto key = requestKey(r.client_id(), r.request_id());
                if (executedRequests_.insert(r.client_id(), r.request_id()).second) {
                    const auto& tx = r.transaction();
                    if (!tx.from().empty() && !tx.to().empty() && tx.amount() > 0)
                        applyTransfer(tx.from(), tx.to(), tx.amount());
                    ++executed;
                }
                auto pending = pendingRequests_.find(key);
                if (pending != pendingRequests_.end()) {
                    pendingBytes_ -= pending->second.bytes;
                    pendingRequests_.erase(pending);
                }
                completed.push_back(key);
                if (r.client_id().empty()) continue;
                auto& reply = replies[r.client_id()].emplace_back();
                reply.set_client_id(r.client_id());
                reply.set_request_id(r.request_id());
                reply.set_view(view);
                reply.set_replica_id(nodeId);
                reply.set_leader_id(leader);
                reply.set_result("success");
            }
        }
        for (auto& [clientId, queue] : replies)
            repliesSent_.fetch_add(clientStreams_.reply(clientId, queue));
        completed.push_back("seq:" + std::to_string(seq));
        const auto watchedBefore = requestTimer_.watch();
        requestTimer_.complete(completed);
        // PBFT restores the timeout when the request it was timing executes -
        // and only if it executed in time. Both halves matter. Resetting on any
        // execution undoes the backoff while the incoming primary is merely
        // draining a backlog, and resetting on a late completion is just as
        // wrong: under a deep backlog every watched entry completes eventually,
        // far past its deadline, so the backoff oscillates between zero and one
        // and never grows enough to outlast the backlog. A replica then keeps
        // electing after the fault that caused the backlog is long gone.
        //
        // Completing before the deadline is the only evidence that the current
        // timeout is adequate, which is exactly what the backoff exists to
        // find. The watch is re-selected, and its generation therefore changes,
        // when the entry being timed completes.
        if (executed && !inViewChange) {
            const auto watchedAfter = requestTimer_.watch();
            const bool watchedCompleted = !watchedBefore || !watchedAfter ||
                                          watchedAfter->generation != watchedBefore->generation;
            // A floored deadline was granted, not earned: right after a view
            // change every completion meets it, which would reset the backoff
            // on every view change and leave a storm to run forever. Draining
            // the queue entirely is evidence either way - nothing is starving.
            const bool inTime = !watchedBefore || !watchedAfter ||
                                (!watchedBefore->floored && Clock::now() < watchedBefore->deadline);
            if (watchedCompleted && inTime) setViewChangeBackoff(0);
        }
        executeUs_ += static_cast<uint64_t>(nowUs() - executeStart);
        markOperationProcessed(seq, path, executed, batch.requests_size());
        return true;
    }
    PrePrepareInfo info;
    {
        std::lock_guard<std::mutex> lk(prePrepareMtx);
        auto it = prePrepareIndex.find(seq);
        if (it == prePrepareIndex.end()) {
            static std::atomic<uint64_t> unknown{0};
            const uint64_t count = unknown.fetch_add(1) + 1;
            if (count == 1 || count % 100 == 0) {
                LOG_WARN("cannot execute seq " << seq << ": no PrePrepare stored (" << count << " so far)");
            }
            return false;
        }
        info = it->second;
        prePrepareIndex.erase(it);
    }

    const std::string key = info.clientId.empty() ? std::string() : requestKey(info.clientId, info.requestId);
    const bool duplicate = !key.empty() && executedRequests_.count(key) != 0;
    if (!duplicate && !info.from.empty() && !info.to.empty() && info.amount > 0) {
        updateBalances(info.from, info.to, info.amount);
    }
    if (!key.empty() && !duplicate) {
        executedRequests_.insert(key);
        executedRequestBySeq_[seq] = key;
    }
    commitOperations[seq] = info.operation;
    markOperationProcessed(seq, path);
    if (!key.empty()) {
        pendingRequests_.erase(key);
        requestTimer_.complete(key);
    }
    requestTimer_.complete("seq:" + std::to_string(seq));
    if (!key.empty()) {
        replyToClient(info.clientId, info.requestId, "success");
    }
    return true;
}
