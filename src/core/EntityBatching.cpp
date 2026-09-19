#include "core/Entity.h"
#include "core/Log.h"
#include "core/events/Message.h"
#include <algorithm>
#include <unordered_set>

void Entity::submitClientRequest(const bedrock::ClientRequest& request) {
    auto reject = [&](const char* reason) {
        ++rejectedRequests_;
        bedrock::ClientReply reply;
        reply.set_client_id(request.client_id()); reply.set_request_id(request.request_id());
        reply.set_replica_id(nodeId); reply.set_view(-1); reply.set_leader_id(-1); reply.set_result(reason);
        clientStreams_.reply(request.client_id(), reply);
    };
    const auto bytes = request.ByteSizeLong() + 8;
    if (request.client_id().empty() || request.request_id() == 0 || request.operation().empty() ||
        !request.has_transaction() || bytes > static_cast<size_t>(options_.batchMaxBytes)) {
        reject("invalid_request");
        return;
    }
    bool accepted = false;
    {
        std::lock_guard<std::mutex> lock(ingressMtx_);
        if (ingress_.size() < static_cast<size_t>(options_.maxPendingRequests) &&
            ingressBytes_ + bytes <= static_cast<size_t>(options_.maxPendingBytes)) {
            ingress_.emplace_back(request, nowUs());
            ingressBytes_ += bytes;
            accepted = true;
        }
    }
    if (!accepted) {
        reject("overloaded");
    }
}

void Entity::acceptClientRequest(const bedrock::ClientRequest& request, long long arrival) {
    const auto key = requestKey(request.client_id(), request.request_id());
    const auto bytes = request.ByteSizeLong() + 8;
    if (request.client_id().empty() || request.request_id() == 0 || request.operation().empty() ||
        !request.has_transaction() || bytes > static_cast<size_t>(options_.batchMaxBytes)) {
        ++rejectedRequests_;
        replyToClient(request.client_id(), request.request_id(), "invalid_request");
        return;
    }
    if (executedRequests_.count(request.client_id(), request.request_id())) {
        replyToClient(request.client_id(), request.request_id(), "success");
        return;
    }
    if (pendingRequests_.count(key)) return;
    if (pendingRequests_.size() >= static_cast<size_t>(options_.maxPendingRequests) ||
        pendingBytes_ + bytes > static_cast<size_t>(options_.maxPendingBytes)) {
        ++rejectedRequests_;
        replyToClient(request.client_id(), request.request_id(), "overloaded");
        return;
    }
    PendingRequest pending;
    pending.clientId = request.client_id(); pending.requestId = request.request_id();
    pending.wire = request; pending.bytes = bytes; pending.arrivalUs = arrival;
    pending.acceptedAt = Clock::now();
    if (!inViewChange) requestTimer_.track(key, pending.acceptedAt);
    pendingRequests_.emplace(key, std::move(pending));
    pendingBytes_ += bytes;
    proposalQueue_.push_back(key);
}

void Entity::scheduleProposalTick() {
    if (!pbftCore_ || !running) return;
    const auto generation = proposalGeneration_;
    scheduler_.schedule(nextProposalTick_, [this, generation] {
        // The engine lock is recursive, so this is the only place the tick can
        // observe how long it waited for it.
        const auto waitStart = nowUs();
        std::lock_guard<std::recursive_mutex> lock(eventMtx);
        tickWaitUs_ += static_cast<uint64_t>(nowUs() - waitStart);
        if (!running || generation != proposalGeneration_) return;
        proposalTick();
        const auto period = std::chrono::milliseconds(options_.proposalIntervalMs);
        nextProposalTick_ += period;
        // Preserve the cadence without replaying missed ticks in a burst.
        const auto now = Clock::now();
        if (nextProposalTick_ <= now)
            nextProposalTick_ += period * ((now - nextProposalTick_) / period + 1);
        scheduleProposalTick();
    });
}

void Entity::proposalTick() {
    std::lock_guard<std::recursive_mutex> lock(eventMtx);
    if (!pbftCore_) return;
    ++proposalTicks_;
    std::deque<std::pair<bedrock::ClientRequest, long long>> incoming;
    {
        std::lock_guard<std::mutex> ingressLock(ingressMtx_);
        incoming.swap(ingress_); ingressBytes_ = 0;
    }
    requestsReceived_ += incoming.size();
    const auto admitStart = nowUs();
    for (const auto& [request, arrival] : incoming) acceptClientRequest(request, arrival);
    admitUs_ += static_cast<uint64_t>(nowUs() - admitStart);
    // Followers do not need an ordering queue. Rebuild it upon leadership.
    if (!isCurrentLeader()) {
        proposalQueue_.clear();
        if (!inViewChange) forwardPendingRequests();
        return;
    }
    if (!inViewChange) proposeBatch();
}

void Entity::proposeBatch() {
    if (!isCurrentLeader() || inViewChange ||
        nextSequenceNumber.load() >= stableCheckpoint_ + consensusWindow() ||
        nextSequenceNumber.load() - lastExecuted_ >= options_.maxInflightBatches) return;
    const auto assembleStart = nowUs();
    bedrock::ProtocolEnvelope env;
    auto* proposal = env.mutable_pre_prepare();
    size_t bytes = 0;
    long long oldestArrival = nowUs();
    // Give ordinary ingress and each peer's reserved relay batch a turn.
    // A busy client queue cannot reject work already admitted by a backup;
    // a busy (or faulty) forwarding peer cannot starve ordinary ingress.
    // Relayed requests are deduplicated against executedRequests_ and
    // pendingRequests_, which between them already cover everything ordered or
    // in flight, plus this set of the keys taken from relays during this tick
    // so two peers relaying the same request cannot both place it. Scanning the
    // log instead cost a heap-allocated key per in-flight request, on every
    // tick for as long as any relay was outstanding.
    std::unordered_set<std::string> relayed;
    for (int attempt = 0; attempt < committeeSize() &&
         proposal->requests_size() < options_.batchMaxRequests && bytes < static_cast<size_t>(options_.batchMaxBytes); ++attempt) {
        const int source = nextProposalSource_;
        nextProposalSource_ = (nextProposalSource_ + 1) % committeeSize();
        if (source == nodeId) {
            while (!proposalQueue_.empty() && proposal->requests_size() < options_.batchMaxRequests) {
                const auto found = pendingRequests_.find(proposalQueue_.front());
                if (found == pendingRequests_.end() || found->second.proposedInView == currentView()) {
                    proposalQueue_.pop_front(); continue;
                }
                auto& request = found->second;
                if (bytes + request.bytes > static_cast<size_t>(options_.batchMaxBytes)) break;
                *proposal->add_requests() = request.wire;
                bytes += request.bytes;
                oldestArrival = std::min(oldestArrival, request.arrivalUs);
                request.proposedInView = currentView();
                proposalQueue_.pop_front();
            }
        } else {
            auto found = forwardedRequests_.find(source);
            if (found == forwardedRequests_.end()) continue;
            auto& requests = found->second;
            while (!requests.empty() && proposal->requests_size() < options_.batchMaxRequests) {
                const auto& request = requests.front();
                const auto key = requestKey(request.client_id(), request.request_id());
                if (executedRequests_.count(request.client_id(), request.request_id()) ||
                    pendingRequests_.count(key) || relayed.count(key)) {
                    requests.pop_front(); continue;
                }
                const auto size = request.ByteSizeLong() + 8;
                if (bytes + size > static_cast<size_t>(options_.batchMaxBytes)) break;
                *proposal->add_requests() = request;
                bytes += size;
                ++relayedRequests_;
                relayed.insert(key);
                requests.pop_front();
            }
            if (requests.empty()) forwardedRequests_.erase(found);
        }
    }
    assembleUs_ += static_cast<uint64_t>(nowUs() - assembleStart);
    if (proposal->requests().empty()) return;
    ++batchesProposed_;
    const auto proposeStart = nowUs();
    const int seq = allocateNextSequence();
    proposal->set_view(currentView()); proposal->set_sequence(seq);
    proposal->set_message_sender_id(nodeId); proposal->set_type("PrePrepare");
    noteFirstSeen(seq, oldestArrival);
    env.set_digest(bedrock::requestDigest(*proposal));
    signEnvelope(env);
    proposeUs_ += static_cast<uint64_t>(nowUs() - proposeStart);
    const auto delay = proposalDelay_->delayForProposal(nodeId, currentLeader(), peerIds_);
    if (delay.count() <= 0) {
        handleConsensusEnvelope(env, /*selfBuilt=*/true);
        sendProtocolToAll(env);
    } else {
        ++delayedProposals_;
        const auto generation = proposalGeneration_;
        scheduler_.scheduleAfter(delay, [this, generation, env = std::move(env)] {
            std::lock_guard<std::recursive_mutex> lock(eventMtx);
            if (!running || generation != proposalGeneration_ || inViewChange || !isCurrentLeader()) return;
            handleConsensusEnvelope(env, /*selfBuilt=*/true);
            sendProtocolToAll(env);
        });
    }
}

void Entity::rebuildProposalQueue() {
    std::vector<std::pair<Clock::time_point, std::string>> order;
    for (const auto& [key, request] : pendingRequests_)
        if (request.proposedInView != currentView()) order.emplace_back(request.acceptedAt, key);
    std::sort(order.begin(), order.end());
    proposalQueue_.clear();
    for (const auto& [time, key] : order) proposalQueue_.push_back(key);
}

void Entity::forwardPendingRequests() {
    const auto watch = requestTimer_.watch();
    if (!watch) return;
    const auto watched = pendingRequests_.find(watch->key);
    if (watched == pendingRequests_.end()) return;
    // Relaying is a liveness measure against a leader that never received a
    // request, not a data path: the client broadcasts, so the leader almost
    // always holds it already. A proposal that carries the watched request
    // settles the question outright - it is waiting its turn, and phase
    // retransmission and certified recovery cover it from here.
    if (watched->second.proposedInView == currentView()) return;
    const auto now = Clock::now();
    // Otherwise wait for execution to have stalled, and relay a given watched
    // entry once, retrying only if it stays stuck. Relaying on every tick
    // sends a batch several times a second to a leader that is already
    // behind, which is how a slow leader becomes a stopped one.
    //
    // These deadlines are deliberately not derived from the election timeout.
    // That timeout is the quantity the learning agent varies, and keying relay
    // aggressiveness to it makes one action change two things at once, so the
    // effect of the timeout alone stops being identifiable. The maintenance
    // interval is the engine's existing answer to "how long may execution
    // stall before a replica acts", and the retransmission gate already uses
    // it for the same judgement.
    const auto grace = std::chrono::milliseconds(bedrock::kMaintenanceIntervalMs);
    if (now - watch->since < grace) return;
    const auto retry = std::chrono::milliseconds(2 * bedrock::kMaintenanceIntervalMs);
    if (watch->key == lastForwardedKey_ && lastRequestForward_ != Clock::time_point{} &&
        now - lastRequestForward_ < retry) return;
    bedrock::ProtocolEnvelope payload;
    auto* batch = payload.mutable_pre_prepare();
    size_t bytes = 0;
    for (const auto& key : requestTimer_.oldestKeys(options_.batchMaxRequests + consensusWindow())) {
        const auto found = pendingRequests_.find(key);
        if (found == pendingRequests_.end() || found->second.proposedInView == currentView()) continue;
        if (bytes + found->second.bytes > static_cast<size_t>(options_.batchMaxBytes) ||
            batch->requests_size() >= options_.batchMaxRequests) break;
        *batch->add_requests() = found->second.wire;
        bytes += found->second.bytes;
    }
    if (batch->requests().empty()) return;
    lastRequestForward_ = now;
    lastForwardedKey_ = watch->key;
    ++relaysSent_;
    LOG_DEBUG("relaying " << batch->requests_size() << " pending requests to leader " << currentLeader()
              << " (watched " << watch->key << " for "
              << std::chrono::duration_cast<std::chrono::milliseconds>(now - watch->since).count() << " ms)");
    sendTo(currentLeader(), Message(signControl(nlohmann::json{{"type", "ForwardRequests"},
        {"view", currentView()}, {"requests", bedrock::encodeEnvelope(payload)}}).dump()));
}

void Entity::acceptForwardedRequests(const nlohmann::json& message) {
    // Called only after the control envelope's replica signature is verified.
    if (inViewChange || !isCurrentLeader() || message.value("view", -1) != currentView()) return;
    const int sender = message.at("message_sender_id");
    if (sender == nodeId || forwardedRequests_.count(sender)) return;
    if (!message.contains("requests") || !message["requests"].is_string() ||
        message["requests"].get_ref<const std::string&>().size() > 2 * (size_t(options_.batchMaxBytes) + 128)) return;
    const auto payload = bedrock::decodeEnvelope(message["requests"]);
    if (!payload.has_pre_prepare() || payload.pre_prepare().requests_size() > options_.batchMaxRequests) return;
    std::deque<bedrock::ClientRequest> requests;
    std::unordered_set<std::string> keys;
    size_t bytes = 0;
    for (const auto& request : payload.pre_prepare().requests()) {
        bytes += request.ByteSizeLong() + 8;
        if (request.client_id().empty() || !request.request_id() || request.operation().empty() ||
            !request.has_transaction() || bytes > static_cast<size_t>(options_.batchMaxBytes)) return;
        const auto key = requestKey(request.client_id(), request.request_id());
        if (!keys.insert(key).second) return;
        if (!executedRequests_.count(request.client_id(), request.request_id()) &&
            !pendingRequests_.count(key)) requests.push_back(request);
    }
    if (!requests.empty()) {
        ++relaysAccepted_;
        forwardedRequests_.emplace(sender, std::move(requests));
    }
}
