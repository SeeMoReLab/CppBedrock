#include "core/Entity.h"
#include "core/Log.h"
#include "core/crypto/CryptoUtils.h"
#include "core/events/ProtoMessage.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_set>

using json = nlohmann::json;
using bedrock::decodeEnvelope;

bool Entity::validateCheckpoint(const json& proof) {
    if (!proof.is_object() || !proof.contains("sequence") || !proof.contains("digest") ||
        !proof.contains("votes") || !proof["votes"].is_array()) return false;
    const int seq = proof.at("sequence");
    const std::string digest = proof.at("digest");
    if (seq == 0) {
        const json genesis{{"balances", json::object()}, {"executed", json::object()}};
        return proof["votes"].empty() && digest == computeSHA256(genesis.dump());
    }
    if (seq < 0 || seq > std::numeric_limits<int>::max() - bedrock::kConsensusWindow ||
        seq % bedrock::kCheckpointInterval || digest.size() != 64 ||
        proof["votes"].size() < static_cast<size_t>(2 * f + 1) ||
        proof["votes"].size() > static_cast<size_t>(committeeSize())) return false;
    std::set<int> signers;
    for (const auto& vote : proof["votes"]) {
        if (vote.value("type", "") != "Checkpoint" || vote.at("sequence").get<int>() != seq ||
            vote.at("digest").get<std::string>() != digest || !verifyControl(vote) ||
            !signers.insert(vote.at("message_sender_id").get<int>()).second) return false;
    }
    return true;
}

void Entity::makeCheckpoint() {
    json snapshot;
    {
        std::lock_guard<std::mutex> lock(balancesMutex);
        snapshot["balances"] = balances;
    }
    // Order independent of local arrival order, including the replay table.
    snapshot["executed"] = executedRequests_.snapshot();
    checkpointSnapshots_[lastExecuted_] = snapshot;
    auto vote = signControl(json{{"type", "Checkpoint"}, {"sequence", lastExecuted_},
                                {"digest", computeSHA256(snapshot.dump())}});
    sendToAll(Message(vote.dump()));
    acceptCheckpoint(vote);
}

void Entity::acceptCheckpoint(const json& msg) {
    if (!verifyControl(msg)) return;
    const int seq = msg.at("sequence"), sender = msg.at("message_sender_id");
    const std::string digest = msg.at("digest");
    if (seq <= stableCheckpoint_ || seq % bedrock::kCheckpointInterval || digest.size() != 64 ||
        static_cast<int64_t>(seq) > static_cast<int64_t>(stableCheckpoint_) + bedrock::kConsensusWindow) return;
    auto& byDigest = checkpointVotes_[seq];
    for (const auto& [otherDigest, votes] : byDigest)
        if (otherDigest != digest && votes.count(sender)) return;
    auto& votes = byDigest[digest];
    votes.emplace(sender, msg);
    if (votes.size() < static_cast<size_t>(2 * f + 1)) return;
    json evidence = json::array();
    for (const auto& [id, vote] : votes) evidence.push_back(vote);
    stabilizeCheckpoint(json{{"sequence", seq}, {"digest", digest}, {"votes", evidence}});
}

void Entity::stabilizeCheckpoint(const json& proof) {
    const int seq = proof.at("sequence");
    if (seq <= stableCheckpoint_) return;
    auto snapshot = checkpointSnapshots_.find(seq);
    if (seq > lastExecuted_ || snapshot == checkpointSnapshots_.end()) { requestRecovery(); return; }
    if (computeSHA256(snapshot->second.dump()) != proof.at("digest").get<std::string>())
        throw std::runtime_error("local state disagrees with a certified checkpoint");
    stableSnapshot_ = snapshot->second;
    stableCheckpointProof_ = proof;
    stableCheckpoint_ = seq;
    pruneCertifiedPrefix();
    sendToAll(Message(signControl(json{{"type", "StableCheckpoint"}, {"checkpoint", proof}}).dump()));
}

void Entity::pruneCertifiedPrefix() {
    const int low = stableCheckpoint_;
    auto ordered = [low](auto& values) { values.erase(values.begin(), values.upper_bound(low)); };
    ordered(batchIndex_);
    ordered(consensusInstances_); ordered(preparedProofs_); ordered(fastVoteProofs_);
    ordered(committedProofs_); ordered(readySequences_); ordered(sequenceStates);
    ordered(executedRequestBySeq_); ordered(checkpointVotes_); ordered(lastBatchRequest_);
    checkpointSnapshots_.erase(checkpointSnapshots_.begin(), checkpointSnapshots_.lower_bound(low));
    auto unordered = [low](auto& values) {
        for (auto it = values.begin(); it != values.end();) {
            if (it->first <= low) it = values.erase(it); else ++it;
        }
    };
    unordered(commitOperations);
    {
        std::lock_guard<std::mutex> lock(prePrepareMtx);
        unordered(prePrepareIndex);
    }
    {
        std::lock_guard<std::mutex> lock(phaseTsMtx);
        unordered(phaseTs_preprepare); unordered(phaseTs_prepare); unordered(phaseTs_commit); unordered(firstSeenUs);
    }
    {
        std::lock_guard<std::mutex> lock(processedMtx);
        processedOperations.erase(processedOperations.begin(), processedOperations.upper_bound(low));
        pruneFloor_ = low + 1;
    }
    // executedRequests_ is application state, included in the checkpoint.
    // Removing entries merely because their log slots were pruned permits replay.
}

void Entity::requestRecovery() {
    const auto now = Clock::now();
    if (lastRecoveryRequest_ != Clock::time_point{} && now - lastRecoveryRequest_ < std::chrono::seconds(1)) return;
    lastRecoveryRequest_ = now;
    sendToAll(Message(signControl(json{{"type", "RecoveryRequest"}, {"after", lastExecuted_}}).dump()));
}

void Entity::installRecovery(const json& msg) {
    const auto& checkpoint = msg.at("checkpoint");
    const auto& snapshot = msg.at("snapshot");
    const auto& proofs = msg.at("committed");
    if (!validateCheckpoint(checkpoint) || !proofs.is_array() || proofs.size() > bedrock::kConsensusWindow) return;
    const int cp = checkpoint.at("sequence");
    const bool needsSnapshot = cp > lastExecuted_;
    if (needsSnapshot || !snapshot.is_null()) {
        if (!snapshot.is_object() || !snapshot.at("balances").is_object() || !snapshot.at("executed").is_object() ||
            computeSHA256(snapshot.dump()) != checkpoint.at("digest").get<std::string>()) return;
    }
    std::set<int> sequences;
    for (const auto& proof : proofs) {
        const int seq = decodeEnvelope(proof.at("proposal")).pre_prepare().sequence();
        if (seq <= cp || static_cast<int64_t>(seq) > static_cast<int64_t>(cp) + bedrock::kConsensusWindow ||
            !sequences.insert(seq).second) return;
        // Replies may have queued during a pause. Already executed decisions
        // are neither installed again nor allowed to hold up fresh evidence.
        if (seq > lastExecuted_ && !validateCommittedProof(proof)) return;
    }
    // Validate the entire response before changing application state.
    if (needsSnapshot) {
        auto restoredBalances = snapshot.at("balances").get<std::map<std::string, int>>();
        auto restoredExecuted = bedrock::ExecutedRequests::restore(snapshot.at("executed"));
        {
            std::lock_guard<std::mutex> lock(balancesMutex);
            balances = std::move(restoredBalances);
        }
        executedRequests_ = std::move(restoredExecuted);
        committedTransactions_.store(executedRequests_.size());
        lastExecuted_ = cp;
        highestCommittedSeq_ = cp;
        nextSequenceNumber.store(std::max(nextSequenceNumber.load(), cp));
        entityInfo["sequence"] = std::max(entityInfo["sequence"].get<int>(), cp);
        std::vector<std::string> completed;
        for (auto it = pendingRequests_.begin(); it != pendingRequests_.end();) {
            if (!executedRequests_.count(it->first)) { ++it; continue; }
            completed.push_back(it->first);
            replyToClient(it->second.clientId, it->second.requestId, "success");
            pendingBytes_ -= it->second.bytes;
            it = pendingRequests_.erase(it);
        }
        // Remove recovered work atomically. An uncompleted watched request
        // keeps its deadline; unrelated recovery must not mask starvation.
        for (const auto& [seq, batch] : batchIndex_)
            if (seq <= cp) completed.push_back("seq:" + std::to_string(seq));
        requestTimer_.complete(completed);
    }
    if (cp > stableCheckpoint_) {
        const auto local = checkpointSnapshots_.find(cp);
        const json& checkpointState = snapshot.is_null() && local != checkpointSnapshots_.end() ? local->second : snapshot;
        if (checkpointState.is_null() || computeSHA256(checkpointState.dump()) != checkpoint.at("digest").get<std::string>())
            throw std::runtime_error("checkpoint state is unavailable or disagrees with its certificate");
        stableCheckpoint_ = cp;
        stableCheckpointProof_ = checkpoint;
        stableSnapshot_ = checkpointState;
        checkpointSnapshots_[cp] = checkpointState;
        pruneCertifiedPrefix();
    }
    for (const auto& proof : proofs) {
        if (decodeEnvelope(proof.at("proposal")).pre_prepare().sequence() <= lastExecuted_) continue;
        int path = -1;
        if (protocolName_ == "SBFT") path = decodeEnvelope(proof.at("certificate")).commit().fast_path() ? 1 : 0;
        learnCommitted(proof, path);
    }
    drainExecution();
    if (!pendingNewView_.is_null() && pendingNewView_["checkpoint"].at("sequence").get<int>() <= lastExecuted_) {
        const auto nv = pendingNewView_;
        finishNewView(nv);
    }
}

void Entity::handleConsensusControl(const json& msg) {
    const auto type = msg.value("type", "");
    if (type == "ViewChange") { onViewChangeMessage(msg); return; }
    if (type == "NewView") { onNewViewMessage(msg); return; }
    if (!verifyControl(msg)) return;
    if (type == "ForwardRequests") acceptForwardedRequests(msg);
    else if (type == "Checkpoint") acceptCheckpoint(msg);
    else if (type == "StableCheckpoint") {
        if (validateCheckpoint(msg.at("checkpoint"))) stabilizeCheckpoint(msg.at("checkpoint"));
    } else if (type == "RecoveryRequest") {
        const int after = msg.at("after");
        if (after < 0) return;
        const int sender = msg.at("message_sender_id");
        json proofs = json::array();
        for (const auto& [seq, proof] : committedProofs_) if (seq > after) proofs.push_back(proof);
        auto response = signControl(json{{"type", "RecoveryResponse"}, {"checkpoint", stableCheckpointProof_},
            // Only replicas missing the checkpoint prefix need the full
            // application snapshot. Ordinary suffix repair sends proofs only.
            {"snapshot", after < stableCheckpoint_ ? stableSnapshot_ : json()}, {"committed", proofs}});
        sendTo(sender, Message(response.dump()));
        if (!lastNewView_.is_null()) sendTo(sender, Message(lastNewView_.dump()));
    } else if (type == "BatchRequest") {
        // Serve the bytes behind a digest. The requester authenticates them
        // against the digest its own certified evidence names, so this
        // response needs no consensus authority of its own.
        const int seq = msg.at("sequence");
        const std::string digest = msg.at("digest");
        auto found = batchIndex_.find(seq);
        if (found == batchIndex_.end() || bedrock::requestDigest(found->second) != digest) return;
        bedrock::ProtocolEnvelope payload;
        *payload.mutable_pre_prepare() = found->second;
        sendTo(msg.at("message_sender_id").get<int>(),
               Message(signControl(json{{"type", "BatchResponse"}, {"sequence", seq}, {"digest", digest},
                                        {"batch", bedrock::encodeEnvelope(payload)}}).dump()));
    } else if (type == "BatchResponse") {
        const int seq = msg.at("sequence");
        if (!msg.contains("batch") || !msg["batch"].is_string() ||
            msg["batch"].get_ref<const std::string&>().size() > 2 * (size_t(options_.batchMaxBytes) + 256)) return;
        const auto payload = bedrock::decodeEnvelope(msg["batch"]);
        if (!payload.has_pre_prepare()) return;
        const auto& body = payload.pre_prepare();
        if (body.requests_size() > options_.batchMaxRequests ||
            body.ByteSizeLong() > static_cast<size_t>(options_.batchMaxBytes) + 128) return;
        std::unordered_set<std::string> keys;
        for (const auto& r : body.requests())
            if (r.client_id().empty() || r.request_id() == 0 || r.operation().empty() || !r.has_transaction() ||
                !keys.insert(requestKey(r.client_id(), r.request_id())).second) return;
        installBatchBody(seq, body);
    } else if (type == "RecoveryResponse") installRecovery(msg);
    // Raw JSON phase messages intentionally have no route into consensus.
}

void Entity::scheduleConsensusMaintenance() {
    if (!pbftCore_ || !running) return;
    scheduler_.scheduleAfter(std::chrono::milliseconds(250), [this] {
        std::lock_guard<std::recursive_mutex> lock(eventMtx);
        if (!running) return;
        maintainConsensus();
        scheduleConsensusMaintenance();
    });
}

void Entity::maintainConsensus() {
    std::lock_guard<std::recursive_mutex> lock(eventMtx);
    if (!pbftCore_) return;
    ++maintenanceTick_;
    if (inViewChange) {
        auto view = viewChangeMsgs_.find(currentView());
        if (view != viewChangeMsgs_.end() && view->second.count(nodeId))
            sendToAll(Message(view->second.at(nodeId).dump()));
        // A paused replica may already have requested a higher view while
        // the other replicas are still making progress in the old view.
        // Certified decisions remain valid and must be recovered while waiting.
        requestRecovery();
    } else {
        for (const auto& [seq, instance] : consensusInstances_) {
            if (seq <= lastExecuted_) continue;
            // A batch is orders of magnitude larger than a vote. Resend it
            // only to replicas whose vote is missing, and not every tick.
            if (isCurrentLeader() && instance.proposal.has_pre_prepare() && maintenanceTick_ % 4 == 0)
                for (int peer : peerIds_)
                    if (peer != nodeId && !instance.prepares.count(peer))
                        sendProtocolTo(peer, instance.proposal);
            if (!bedrock::carriesBody(instance.proposal)) requestBatch(seq);
            const auto prepare = instance.prepares.find(nodeId);
            if (prepare != instance.prepares.end()) {
                if (protocolName_ == "PBFT") sendProtocolToAll(prepare->second);
                else if (!isCurrentLeader()) sendProtocolTo(currentLeader(), prepare->second);
            }
            if (protocolName_ == "PBFT") {
                const auto commit = instance.commits.find(nodeId);
                if (commit != instance.commits.end()) sendProtocolToAll(commit->second);
            } else {
                if (isCurrentLeader()) {
                    if (instance.certificate.has_commit()) sendProtocolToAll(instance.certificate);
                    if (instance.execution.has_commit()) sendProtocolToAll(instance.execution);
                } else if (instance.acks.count(nodeId)) sendProtocolTo(currentLeader(), instance.acks.at(nodeId));
            }
        }
        // A certified decision whose batch never arrived blocks execution
        // until the bytes are fetched.
        auto next = readySequences_.find(lastExecuted_ + 1);
        if (next != readySequences_.end() && !batchIndex_.count(next->first)) requestBatch(next->first);
        if (Clock::now() - lastConsensusProgress_ > std::chrono::seconds(1) &&
            (!pendingRequests_.empty() || !readySequences_.empty() ||
             (!consensusInstances_.empty() && consensusInstances_.rbegin()->first > lastExecuted_) ||
             entityInfo["sequence"].get<int>() > lastExecuted_)) requestRecovery();
    }
    if (stableCheckpoint_ > 0)
        sendToAll(Message(signControl(json{{"type", "StableCheckpoint"}, {"checkpoint", stableCheckpointProof_}}).dump()));
    // A dropped checkpoint vote must not permanently block the log window.
    for (const auto& [seq, byDigest] : checkpointVotes_)
        for (const auto& [digest, votes] : byDigest)
            if (votes.count(nodeId)) sendToAll(Message(votes.at(nodeId).dump()));
}
