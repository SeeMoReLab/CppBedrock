// Authenticated view changes for PBFT and SBFT (c=0). Recovery starts at a
// certified checkpoint, never at an individual replica's highest commit.
#include "core/Entity.h"
#include "core/Log.h"
#include "core/events/ProtoMessage.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

using json = nlohmann::json;
using bedrock::ProtocolEnvelope;
using bedrock::encodeEnvelope;
using bedrock::decodeEnvelope;

namespace {
int proofSequence(const json& proof) { return decodeEnvelope(proof.at("proposal")).pre_prepare().sequence(); }
int proofView(const json& proof) { return decodeEnvelope(proof.at("proposal")).pre_prepare().view(); }
}

void Entity::onRequestTimerExpired(std::uint64_t generation) {
    std::lock_guard<std::recursive_mutex> engine(eventMtx);
    if (!running || !pbftCore_ || inViewChange || !requestTimer_.expired(generation)) return;
    if (currentView() == std::numeric_limits<int>::max()) throw std::runtime_error("view number exhausted");
    startViewChange(currentView() + 1, "watched request deadline expired");
}

std::chrono::milliseconds Entity::viewChangeWaitDuration() const {
    // Saturate safely at the maximum supported millisecond timeout.
    const uint64_t base = std::max(1, viewChangeTimeoutMs.load());
    const uint64_t duration = base << std::min(viewChangeBackoff_, 30u);
    return std::chrono::milliseconds(std::min(duration, uint64_t(std::numeric_limits<int>::max())));
}

void Entity::cancelViewChangeWait() {
    ++viewChangeWaitGeneration_;
    viewChangeWaitSince_.reset();
}

void Entity::armViewChangeWait() {
    viewChangeWaitSince_ = Clock::now();
    scheduleViewChangeWait();
}

void Entity::scheduleViewChangeWait() {
    const auto generation = ++viewChangeWaitGeneration_;
    if (!viewChangeWaitSince_) return;
    scheduler_.schedule(*viewChangeWaitSince_ + viewChangeWaitDuration(), [this, generation] {
        onViewChangeWaitExpired(generation);
    });
}

void Entity::onViewChangeWaitExpired(uint64_t generation) {
    std::lock_guard<std::recursive_mutex> engine(eventMtx);
    if (!running || !inViewChange || generation != viewChangeWaitGeneration_ ||
        !viewChangeWaitSince_ || Clock::now() < *viewChangeWaitSince_ + viewChangeWaitDuration()) return;
    if (currentView() == std::numeric_limits<int>::max()) throw std::runtime_error("view number exhausted");
    viewChangeBackoff_ = std::min(viewChangeBackoff_ + 1, 30u);
    startViewChange(currentView() + 1, "NewView deadline expired");
}

void Entity::startViewChange(int newView, const std::string& reason) {
    if (!pbftCore_ || newView <= currentView()) return;
    entityInfo["view"] = newView;
    inViewChange = true;
    pendingNewView_ = json();
    viewChangesStarted_.fetch_add(1);
    if (agentClient_) agentClient_->recordViewChange();
    cancelViewChangeWait();
    forwardedRequests_.clear();
    lastRequestForward_ = Clock::time_point{};
    lastForwardedKey_.clear();
    cancelDelayedProposals();
    json prepared = json::array(), fastVotes = json::array(), committed = json::array();
    for (const auto& [seq, proof] : preparedProofs_) if (seq > stableCheckpoint_ && !committedProofs_.count(seq)) prepared.push_back(proof);
    for (const auto& [seq, proof] : fastVoteProofs_) if (seq > stableCheckpoint_ && !committedProofs_.count(seq) &&
        (!preparedProofs_.count(seq) || proofView(proof) > proofView(preparedProofs_.at(seq)))) fastVotes.push_back(proof);
    for (const auto& [seq, proof] : committedProofs_) if (seq > stableCheckpoint_) committed.push_back(proof);
    const auto vc = signControl(json{{"type", "ViewChange"}, {"new_view", newView},
        {"checkpoint", stableCheckpointProof_}, {"prepared", prepared}, {"fast_votes", fastVotes}, {"committed", committed}});
    for (auto it = viewChangeMsgs_.begin(); it != viewChangeMsgs_.end();) {
        it->second.erase(nodeId);
        if (it->second.empty()) it = viewChangeMsgs_.erase(it); else ++it;
    }
    viewChangeMsgs_[newView][nodeId] = vc;
    LOG_INFO("view change to " << newView << " (" << reason << "), checkpoint=" << stableCheckpoint_
             << ", prepared=" << prepared.size() << ", committed=" << committed.size()
             << ", evidence_bytes=" << vc.dump().size());
    requestTimer_.clear();
    if (viewChangeMsgs_[newView].size() >= static_cast<size_t>(2 * f + 1)) armViewChangeWait();
    sendToAll(Message(vc.dump()));
    tryBuildNewView(newView);
}

bool Entity::validateViewChange(const json& msg) {
    if (msg.value("type", "") != "ViewChange" || !verifyControl(msg)) return false;
    const int view = msg.value("new_view", -1);
    if (view <= 0 || !msg.contains("checkpoint") || !validateCheckpoint(msg["checkpoint"])) return false;
    const int low = msg["checkpoint"].at("sequence");
    // Header-only evidence: three proof kinds over the window, each a
    // signed header plus at most one vote per replica.
    const uint64_t maxBytes = 3 * uint64_t(bedrock::kConsensusWindow) *
        (4096 + uint64_t(committeeSize()) * 8192) + 65536;
    if (msg.dump().size() > maxBytes) return false;
    std::set<int> uncommitted;
    for (const std::string kind : {"prepared", "fast_votes", "committed"}) {
        if (!msg.contains(kind) || !msg[kind].is_array() || msg[kind].size() > bedrock::kConsensusWindow) return false;
        if (kind == "fast_votes" && protocolName_ != "SBFT" && !msg[kind].empty()) return false;
        std::set<int> sequences;
        for (const auto& proof : msg[kind]) {
            const int seq = proofSequence(proof);
            if (seq <= low || static_cast<int64_t>(seq) > static_cast<int64_t>(low) + bedrock::kConsensusWindow ||
                proofView(proof) >= view || !sequences.insert(seq).second) return false;
            if (kind == "committed" && uncommitted.count(seq)) return false;
            if (kind != "committed") uncommitted.insert(seq);
            if (kind == "prepared" && !validatePreparedProof(proof)) return false;
            if (kind == "committed" && !validateCommittedProof(proof)) return false;
            if (kind == "fast_votes") {
                const auto proposal = decodeEnvelope(proof.at("proposal"));
                const auto vote = decodeEnvelope(proof.at("vote"));
                if (!validateProposal(proposal) || !vote.has_prepare() || vote.prepare().type() != "prepare" ||
                    vote.prepare().message_sender_id() != msg.at("message_sender_id").get<int>() ||
                    vote.prepare().view() != proposal.pre_prepare().view() || vote.prepare().sequence() != seq ||
                    vote.digest() != proposal.digest() || !verifyEnvelope(vote)) return false;
            }
        }
    }
    return true;
}

void Entity::onViewChangeMessage(const json& msg) {
    if (!pbftCore_ || !validateViewChange(msg)) return;
    const int view = msg.at("new_view"), sender = msg.at("message_sender_id");
    if (view < currentView() || (view == currentView() && !inViewChange)) {
        if (!lastNewView_.is_null() && lastNewView_.at("new_view").get<int>() >= view)
            sendTo(sender, Message(lastNewView_.dump()));
        return;
    }
    // At most one outstanding view-change claim per replica prevents an
    // authenticated faulty peer from filling memory with arbitrary views.
    for (auto it = viewChangeMsgs_.begin(); it != viewChangeMsgs_.end();) {
        if (it->first > view && it->second.count(sender)) return;
        if (it->first != view) it->second.erase(sender);
        if (it->second.empty()) it = viewChangeMsgs_.erase(it); else ++it;
    }
    const bool inserted = viewChangeMsgs_[view].emplace(sender, msg).second;
    if (inserted && view == currentView() && inViewChange &&
        viewChangeMsgs_[view].size() == static_cast<size_t>(2 * f + 1)) {
        armViewChangeWait();
    }
    std::set<int> higher;
    int smallest = std::numeric_limits<int>::max();
    for (const auto& [candidate, messages] : viewChangeMsgs_) if (candidate > currentView()) {
        smallest = std::min(smallest, candidate);
        for (const auto& [id, vc] : messages) higher.insert(id);
    }
    if (higher.size() >= static_cast<size_t>(f + 1)) {
        startViewChange(smallest, "f+1 authenticated replicas requested higher views");
        return;
    }
    tryBuildNewView(view);
}

json Entity::selectNewView(const json& changes, int view) {
    if (!changes.is_array() || changes.size() < static_cast<size_t>(2 * f + 1) ||
        changes.size() > static_cast<size_t>(committeeSize())) throw std::invalid_argument("NewView lacks a quorum");
    std::set<int> senders;
    json checkpoint = json();
    int low = -1, maxSeq = 0;
    std::map<int, json> prepared, committed;
    std::map<int, std::map<std::string, std::vector<std::pair<int, json>>>> fast;
    for (const auto& vc : changes) {
        if (!validateViewChange(vc) || vc.at("new_view").get<int>() != view ||
            !senders.insert(vc.at("message_sender_id").get<int>()).second)
            throw std::invalid_argument("invalid or duplicate ViewChange evidence");
        const int cp = vc["checkpoint"].at("sequence");
        if (cp > low) { low = cp; checkpoint = vc["checkpoint"]; }
        for (const auto& proof : vc["prepared"]) {
            const int seq = proofSequence(proof);
            auto it = prepared.find(seq);
            if (it == prepared.end() || proofView(it->second) < proofView(proof)) prepared[seq] = proof;
            else if (proofView(it->second) == proofView(proof) &&
                decodeEnvelope(it->second["proposal"]).digest() != decodeEnvelope(proof["proposal"]).digest())
                throw std::invalid_argument("conflicting prepared certificates");
            maxSeq = std::max(maxSeq, seq);
        }
        for (const auto& proof : vc["committed"]) {
            const int seq = proofSequence(proof);
            auto it = committed.find(seq);
            if (it != committed.end() && decodeEnvelope(it->second["proposal"]).digest() != decodeEnvelope(proof["proposal"]).digest())
                throw std::invalid_argument("conflicting committed certificates");
            committed[seq] = proof;
            maxSeq = std::max(maxSeq, seq);
        }
        for (const auto& proof : vc["fast_votes"]) {
            const auto proposal = decodeEnvelope(proof["proposal"]);
            fast[proofSequence(proof)][proposal.digest()].push_back({proofView(proof), proof});
            maxSeq = std::max(maxSeq, proofSequence(proof));
        }
    }
    if (static_cast<int64_t>(maxSeq) > static_cast<int64_t>(low) + bedrock::kConsensusWindow)
        throw std::invalid_argument("NewView exceeds the certified sequence window");
    json proposals = json::array();
    for (int seq = low + 1; seq <= maxSeq; ++seq) {
        json selected;
        if (committed.count(seq)) selected = committed.at(seq);
        else {
            int slowView = -1;
            if (prepared.count(seq)) { selected = prepared.at(seq); slowView = proofView(selected); }
            // SBFT c=0: the (f+1)-th highest fast vote view is the highest
            // view at which a fast decision is still possible. A unique fast
            // candidate overrides the slow certificate only at a higher view.
            int fastView = -1;
            json fastChoice;
            bool tied = false;
            for (auto& [digest, votes] : fast[seq]) {
                if (votes.size() < static_cast<size_t>(f + 1)) continue;
                std::sort(votes.begin(), votes.end(), [](const auto& a, const auto& b) { return a.first > b.first; });
                const int rank = votes[f].first;
                if (rank > fastView) { fastView = rank; fastChoice = votes.front().second; tied = false; }
                else if (rank == fastView) tied = true;
            }
            if (!tied && fastView > slowView) selected = fastChoice;
        }
        // Re-propose the decided batch by the digest its certificate names.
        // Recomputing a digest here would silently redefine the decision.
        ProtocolEnvelope env;
        auto* p = env.mutable_pre_prepare();
        if (!selected.is_null()) {
            const auto chosen = decodeEnvelope(selected["proposal"]);
            *p = chosen.pre_prepare();
            env.set_digest(chosen.digest());
        } else {
            env.set_digest(bedrock::requestDigest(*p)); // Empty batch: the view-change no-op.
        }
        p->clear_requests();
        p->set_type("PrePrepare"); p->set_view(view); p->set_sequence(seq); p->set_message_sender_id(leaderForView(view));
        p->clear_signature(); p->clear_qc(); p->clear_combined_messages();
        proposals.push_back(encodeEnvelope(env));
    }
    return json{{"checkpoint", checkpoint}, {"proposals", proposals}};
}

void Entity::tryBuildNewView(int view) {
    if (!inViewChange || currentView() != view || !isLeaderForView(view) || lastNewViewSent_ >= view) return;
    auto it = viewChangeMsgs_.find(view);
    if (it == viewChangeMsgs_.end() || it->second.size() < static_cast<size_t>(2 * f + 1)) return;
    json changes = json::array();
    for (const auto& [id, vc] : it->second) {
        changes.push_back(vc);
        if (changes.size() == static_cast<size_t>(2 * f + 1)) break;
    }
    const auto selection = selectNewView(changes, view);
    json proposals = json::array();
    for (const auto& encoded : selection["proposals"]) {
        auto env = decodeEnvelope(encoded); signEnvelope(env); proposals.push_back(encodeEnvelope(env));
    }
    auto nv = signControl(json{{"type", "NewView"}, {"new_view", view}, {"view_changes", changes},
        {"checkpoint", selection["checkpoint"]}, {"pre_prepares", proposals}});
    lastNewViewSent_ = view;
    LOG_INFO("NewView " << view << " built from " << changes.size() << " view changes, "
             << proposals.size() << " re-proposals, " << nv.dump().size() << " bytes");
    sendToAll(Message(nv.dump()));
    onNewViewMessage(nv);
}

void Entity::onNewViewMessage(const json& msg) {
    if (!pbftCore_ || msg.value("type", "") != "NewView" || !verifyControl(msg)) return;
    const int view = msg.value("new_view", -1);
    if (view < 0 || msg.at("message_sender_id").get<int>() != leaderForView(view) ||
        view < currentView() || (view == currentView() && !inViewChange)) return;
    if (!msg.at("view_changes").is_array() || msg.at("view_changes").size() != static_cast<size_t>(2 * f + 1)) return;
    const auto selection = selectNewView(msg.at("view_changes"), view);
    if (msg.at("checkpoint") != selection["checkpoint"] || !msg.at("pre_prepares").is_array() ||
        msg["pre_prepares"].size() != selection["proposals"].size()) return;
    for (size_t i = 0; i < msg["pre_prepares"].size(); ++i) {
        auto env = decodeEnvelope(msg["pre_prepares"][i]);
        if (!validateProposal(env)) return;
        env.clear_signature();
        if (encodeEnvelope(env) != selection["proposals"][i]) return;
    }
    // Do not vote in a new view while missing its checkpoint state.
    if (msg["checkpoint"].at("sequence").get<int>() > lastExecuted_) {
        // Repeated NewView messages cannot postpone the recovery deadline.
        if (!pendingNewView_.is_null() && pendingNewView_.at("new_view").get<int>() == view) { requestRecovery(); return; }
        entityInfo["view"] = view;
        inViewChange = true;
        pendingNewView_ = msg;
        cancelDelayedProposals();
        requestTimer_.clear();
        armViewChangeWait();
        requestRecovery();
        return;
    }
    finishNewView(msg);
}

void Entity::finishNewView(const json& msg) {
    const int view = msg.at("new_view");
    entityInfo["view"] = view;
    inViewChange = false;
    cancelViewChangeWait();
    forwardedRequests_.clear();
    lastRequestForward_ = Clock::time_point{};
    lastForwardedKey_.clear();
    lastNewView_ = msg;
    pendingNewView_ = json();
    cancelDelayedProposals();
    // These flags belong to (view, sequence), never just sequence.
    consensusInstances_.clear();
    {
        std::lock_guard<std::mutex> lock(phaseTsMtx);
        phaseTs_preprepare.clear(); phaseTs_prepare.clear(); phaseTs_commit.clear();
    }
    stabilizeCheckpoint(msg.at("checkpoint"));
    std::vector<bedrock::PendingRequestTimer::Entry> pending;
    pending.reserve(pendingRequests_.size());
    for (auto& [key, request] : pendingRequests_) {
        request.proposedInView = -1;
        pending.push_back({key, request.acceptedAt});
    }
    requestTimer_.reset(pending);
    int next = std::max(lastExecuted_, msg["checkpoint"].at("sequence").get<int>());
    // Set the allocator before processing proposals, since execution can
    // stabilize a checkpoint and release buffered client requests.
    for (const auto& encoded : msg["pre_prepares"])
        next = std::max(next, decodeEnvelope(encoded).pre_prepare().sequence());
    nextSequenceNumber.store(next);
    entityInfo["sequence"] = next;
    for (const auto& encoded : msg["pre_prepares"]) installReProposal(encoded, view);
    viewChangeMsgs_.erase(viewChangeMsgs_.begin(), viewChangeMsgs_.upper_bound(view));
    newViewsInstalled_.fetch_add(1);
    if (agentClient_) agentClient_->recordNewView();
    LOG_INFO("installed authenticated view " << view << ", checkpoint=" << stableCheckpoint_ << ", next_sequence=" << next + 1);
    if (isCurrentLeader()) proposeBufferedRequests();
}

void Entity::installReProposal(const json& encoded, int view) {
    auto env = decodeEnvelope(encoded);
    attachLocalBody(env);
    for (const auto& r : env.pre_prepare().requests()) {
        auto pending = pendingRequests_.find(requestKey(r.client_id(), r.request_id()));
        if (pending != pendingRequests_.end()) pending->second.proposedInView = view;
    }
    handleConsensusEnvelope(env);
}
