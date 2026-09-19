#include "core/Entity.h"
#include "core/Log.h"
#include "core/crypto/CryptoUtils.h"
#include "core/events/ProtoMessage.h"

#include <algorithm>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>

using json = nlohmann::json;
using bedrock::ProtocolEnvelope;
using bedrock::encodeEnvelope;
using bedrock::decodeEnvelope;

namespace {
json matchingVotes(const std::map<int, ProtocolEnvelope>& votes, const std::string& digest) {
    json out = json::array();
    for (const auto& [sender, vote] : votes)
        if (vote.digest() == digest) out.push_back(encodeEnvelope(vote));
    return out;
}
} // namespace

void Entity::initializeConsensus() {
    if (!pbftCore_) return;
    if (committeeSize() != 3 * f + 1)
        throw std::runtime_error("PBFT/SBFT require exactly 3f+1 replicas (SBFT c=0)");
    if (!options_.proposalSigning)
        throw std::runtime_error("PBFT/SBFT require authenticated proposals and votes; --proposal-signing false is unsupported");
    if (options_.proposalIntervalMs <= 0 || options_.batchMaxRequests <= 0 ||
        options_.batchMaxRequests > bedrock::kMaxBatchRequests || options_.batchMaxBytes <= 0 ||
        options_.batchMaxBytes > bedrock::kMaxBatchBytes || options_.maxInflightBatches <= 0 ||
        options_.maxInflightBatches > bedrock::kMaxConsensusWindow || options_.maxPendingRequests <= 0 ||
        options_.maxPendingBytes < options_.batchMaxBytes)
        throw std::runtime_error("invalid batching or admission limits");
    // View-change evidence names batches by digest, so its size depends on
    // the committee and the window, never on the batch size.
    if (uint64_t(4 * f + 3) * consensusWindow() *
        uint64_t(committeeSize()) * 16384 > 56 * 1024 * 1024)
        throw std::runtime_error("committee too large for the recovery message budget");
    std::string keys;
    for (int id : peerIds_) {
        std::ifstream file(options_.keysDir + "/server_" + std::to_string(id) + "_public.pem");
        if (!file) throw std::runtime_error("missing public key for replica " + std::to_string(id));
        std::ostringstream contents; contents << file.rdbuf();
        keys += std::to_string(id) + ":" + contents.str();
    }
    consensusDomain_ = "CppBedrock/consensus/v3/" + protocolName_ + "/" + computeSHA256(keys) + "/" + std::to_string(options_.batchMaxBytes) + "/";
    const auto challenge = cryptoProvider->sign(consensusDomain_);
    if (!cryptoProvider->verify(consensusDomain_, challenge,
            options_.keysDir + "/server_" + std::to_string(nodeId) + "_public.pem"))
        throw std::runtime_error("replica public and private keys do not match");
    stableSnapshot_ = json{{"balances", json::object()}, {"executed", json::object()}};
    stableCheckpointProof_ = json{{"sequence", 0}, {"digest", computeSHA256(stableSnapshot_.dump())},
                                  {"votes", json::array()}};
}

void Entity::signEnvelope(ProtocolEnvelope& env) {
    env.clear_signature();
    env.set_signature(cryptoProvider->sign(consensusDomain_ + "phase/" + bedrock::signedHeaderBytes(env)));
}

bool Entity::verifyEnvelope(const ProtocolEnvelope& env) {
    ProtoMessage p(env);
    if (!committee_.contains(p.sender_id()) || p.view() < 0 || p.sequence() <= 0 ||
        p.sequence() > std::numeric_limits<int>::max() - consensusWindow() ||
        env.digest().size() != 64 || env.signature().empty()) return false;
    return cryptoProvider->verify(consensusDomain_ + "phase/" + bedrock::signedHeaderBytes(env),
        env.signature(), options_.keysDir + "/server_" + std::to_string(p.sender_id()) + "_public.pem");
}

json Entity::signControl(json msg) {
    msg.erase("signature");
    msg["message_sender_id"] = nodeId;
    msg["signature"] = cryptoProvider->sign(consensusDomain_ + "control/" + msg.dump());
    return msg;
}

bool Entity::verifyControl(const json& msg) {
    if (!msg.is_object() || !msg.contains("signature") || !msg["signature"].is_string()) return false;
    const int sender = msg.value("message_sender_id", -1);
    if (!committee_.contains(sender)) return false;
    json body = msg;
    body.erase("signature");
    return cryptoProvider->verify(consensusDomain_ + "control/" + body.dump(), msg["signature"],
        options_.keysDir + "/server_" + std::to_string(sender) + "_public.pem");
}

bool Entity::validateProposal(const ProtocolEnvelope& env, bool selfBuilt) {
    // This replica signed a self-built proposal a moment ago; checking its own
    // signature proves nothing it does not already know.
    if (!env.has_pre_prepare() || (!selfBuilt && !verifyEnvelope(env))) return false;
    const auto& p = env.pre_prepare();
    if (p.type() != "PrePrepare" || p.message_sender_id() != leaderForView(p.view()) ||
        !p.operation().empty() || !p.timestamp().empty() || !p.client_id().empty() ||
        p.request_id() != 0 || p.has_transaction() || !p.signature().empty() ||
        !p.qc().empty() || p.combined_messages_size() || p.requests_size() > options_.batchMaxRequests ||
        p.ByteSizeLong() > static_cast<size_t>(options_.batchMaxBytes) + 128) return false;
    // Header-only evidence is well formed: the leader's signature binds the
    // digest, so the batch can be fetched from any peer and checked against
    // it. A partially filled body never is. Replicas vote and execute only
    // once the named batch is in hand (see advanceConsensus, drainExecution).
    // The digest of a self-built proposal was computed over this exact body in
    // proposeBatch. Everything below still runs: the field, size and duplicate
    // checks are what would catch an assembly mistake, and they are cheap.
    if (!selfBuilt && !bedrock::bodyMatchesDigest(env)) return p.requests_size() == 0;
    // Reject a batch that names the same request twice, without allocating a
    // key per request: views into the batch's own bytes sort just as well, and
    // a batch holds thousands of requests.
    std::vector<std::pair<std::string_view, uint64_t>> named;
    named.reserve(p.requests_size());
    for (const auto& r : p.requests()) {
        if (r.client_id().empty() || r.request_id() == 0 || r.operation().empty() || !r.has_transaction())
            return false;
        named.emplace_back(r.client_id(), r.request_id());
    }
    std::sort(named.begin(), named.end());
    return std::adjacent_find(named.begin(), named.end()) == named.end();
}

std::string Entity::digestFor(int seq) const {
    auto instance = consensusInstances_.find(seq);
    if (instance != consensusInstances_.end() && instance->second.proposal.has_pre_prepare())
        return instance->second.proposal.digest();
    auto committed = committedProofs_.find(seq);
    if (committed != committedProofs_.end()) return decodeEnvelope(committed->second.at("proposal")).digest();
    auto prepared = preparedProofs_.find(seq);
    if (prepared != preparedProofs_.end()) return decodeEnvelope(prepared->second.at("proposal")).digest();
    return std::string();
}

void Entity::attachLocalBody(ProtocolEnvelope& env) const {
    if (bedrock::carriesBody(env)) return;
    auto found = batchIndex_.find(env.pre_prepare().sequence());
    if (found == batchIndex_.end() || bedrock::requestDigest(found->second) != env.digest()) return;
    *env.mutable_pre_prepare()->mutable_requests() = found->second.requests();
}

void Entity::requestBatch(int seq) {
    const auto digest = digestFor(seq);
    if (digest.empty() || seq <= stableCheckpoint_) return;
    const auto now = Clock::now();
    auto& last = lastBatchRequest_[seq];
    if (last != Clock::time_point{} && now - last < std::chrono::milliseconds(250)) return;
    last = now;
    sendToAll(Message(signControl(json{{"type", "BatchRequest"}, {"sequence", seq}, {"digest", digest}}).dump()));
}

void Entity::installBatchBody(int seq, const bedrock::PrePrepare& body) {
    if (seq <= lastExecuted_ || seq <= stableCheckpoint_) return;
    if (bedrock::requestDigest(body) != digestFor(seq)) return;
    auto instance = consensusInstances_.find(seq);
    if (instance != consensusInstances_.end() && instance->second.proposal.has_pre_prepare() &&
        !bedrock::carriesBody(instance->second.proposal))
        *instance->second.proposal.mutable_pre_prepare()->mutable_requests() = body.requests();
    auto& stored = batchIndex_[seq];
    stored = body;
    stored.set_sequence(seq);
    for (const auto& r : body.requests()) {
        auto pending = pendingRequests_.find(requestKey(r.client_id(), r.request_id()));
        if (pending != pendingRequests_.end()) pending->second.proposedInView = currentView();
    }
    LOG_DEBUG("installed fetched batch body for sequence " << seq);
    if (instance != consensusInstances_.end()) advanceConsensus(seq);
    drainExecution();
}

bool Entity::validateCertificate(const ProtocolEnvelope& env, const std::string& phase, int quorum) {
    if (!env.has_commit() || !verifyEnvelope(env)) return false;
    const auto& c = env.commit();
    if (c.message_sender_id() != leaderForView(c.view()) ||
        c.combined_messages_size() < quorum || c.combined_messages_size() > committeeSize()) return false;
    std::set<int> signers;
    for (const auto& share : c.combined_messages()) {
        if (share.view() != c.view() || share.sequence() != c.sequence() || share.digest() != env.digest() ||
            !signers.insert(share.message_sender_id()).second ||
            !verifyEnvelope(bedrock::certificateVote(share, phase))) return false;
    }
    return true;
}

bool Entity::validatePreparedProof(const json& proof) {
    if (!proof.is_object() || !proof.contains("proposal")) return false;
    const auto proposal = decodeEnvelope(proof["proposal"]);
    if (!validateProposal(proposal)) return false;
    const auto& p = proposal.pre_prepare();
    if (protocolName_ == "SBFT") {
        if (!proof.contains("certificate")) return false;
        const auto cert = decodeEnvelope(proof["certificate"]);
        return cert.has_commit() && cert.commit().type() == "commit" &&
            cert.commit().view() == p.view() && cert.commit().sequence() == p.sequence() &&
            cert.digest() == proposal.digest() &&
            validateCertificate(cert, "prepare", cert.commit().fast_path() ? committeeSize() : 2 * f + 1);
    }
    if (!proof.contains("prepares") || !proof["prepares"].is_array() ||
        proof["prepares"].size() < static_cast<size_t>(2 * f) || proof["prepares"].size() > static_cast<size_t>(committeeSize())) return false;
    std::set<int> signers;
    for (const auto& encoded : proof["prepares"]) {
        auto vote = decodeEnvelope(encoded);
        if (!vote.has_prepare() || vote.prepare().type() != "prepare" ||
            vote.prepare().view() != p.view() || vote.prepare().sequence() != p.sequence() ||
            vote.digest() != proposal.digest() || vote.prepare().message_sender_id() == leaderForView(p.view()) ||
            !signers.insert(vote.prepare().message_sender_id()).second || !verifyEnvelope(vote)) return false;
    }
    return true;
}

bool Entity::validateCommittedProof(const json& proof) {
    if (!proof.is_object() || !proof.contains("proposal")) return false;
    auto proposal = decodeEnvelope(proof["proposal"]);
    if (!validateProposal(proposal)) return false;
    const auto& p = proposal.pre_prepare();
    if (protocolName_ == "SBFT") {
        if (!proof.contains("certificate")) return false;
        auto cert = decodeEnvelope(proof["certificate"]);
        if (!cert.has_commit() || cert.digest() != proposal.digest() ||
            cert.commit().view() != p.view() || cert.commit().sequence() != p.sequence()) return false;
        if (cert.commit().type() == "commit" && cert.commit().fast_path())
            return validateCertificate(cert, "prepare", committeeSize());
        return cert.commit().type() == "execute" && !cert.commit().fast_path() &&
            validateCertificate(cert, "commitAck", 2 * f + 1);
    }
    if (!validatePreparedProof(proof) || !proof.contains("commits") || !proof["commits"].is_array() ||
        proof["commits"].size() < static_cast<size_t>(2 * f + 1) || proof["commits"].size() > static_cast<size_t>(committeeSize())) return false;
    std::set<int> signers;
    for (const auto& encoded : proof["commits"]) {
        auto vote = decodeEnvelope(encoded);
        if (!vote.has_commit() || vote.commit().type() != "commit" || vote.commit().fast_path() ||
            vote.commit().combined_messages_size() != 0 || vote.commit().view() != p.view() ||
            vote.commit().sequence() != p.sequence() || vote.digest() != proposal.digest() ||
            !signers.insert(vote.commit().message_sender_id()).second || !verifyEnvelope(vote)) return false;
    }
    return true;
}

void Entity::acceptProposal(const ProtocolEnvelope& env) {
    const auto& p = env.pre_prepare();
    auto& instance = consensusInstances_[p.sequence()];
    if (instance.proposal.has_pre_prepare()) {
        // Conflicting proposals were rejected before this call, so a repeat
        // names the same batch. A leader retransmission is how a replica
        // holding only the new-view header obtains the bytes.
        if (!bedrock::carriesBody(instance.proposal) && bedrock::carriesBody(env)) {
            instance.proposal = env;
            if (p.sequence() > lastExecuted_) batchIndex_[p.sequence()] = p;
        }
        return;
    }
    instance.proposal = env;
    // Only a batch verified against the header's digest enters the log, so
    // holding an entry for a sequence means holding the right bytes. A
    // header-only re-proposal invalidates any body left from another view.
    if (p.sequence() > lastExecuted_) {
        if (bedrock::carriesBody(env)) batchIndex_[p.sequence()] = p;
        else {
            auto stale = batchIndex_.find(p.sequence());
            if (stale != batchIndex_.end() && bedrock::requestDigest(stale->second) != env.digest())
                batchIndex_.erase(stale);
        }
    }
    {
        std::lock_guard<std::mutex> lock(phaseTsMtx);
        phaseTs_preprepare.emplace(p.sequence(), nowUs());
        firstSeenUs.emplace(p.sequence(), nowUs());
    }
    // Mark the requests this batch carries as in flight, exactly as a fetched
    // body does. Without it a replica cannot tell a request waiting its turn
    // in the leader's queue from one the leader never received, and both the
    // relay path and the next proposal answered that question by scanning the
    // whole log and building a key per in-flight request.
    for (const auto& r : p.requests()) {
        auto pending = pendingRequests_.find(requestKey(r.client_id(), r.request_id()));
        if (pending != pendingRequests_.end()) pending->second.proposedInView = p.view();
    }
    entityInfo["sequence"] = std::max(entityInfo["sequence"].get<int>(), p.sequence());
    onPrePrepareAccepted(p.sequence(), p.view());
}

void Entity::handleConsensusEnvelope(const ProtocolEnvelope& env, bool selfBuilt) {
    const auto handleStart = nowUs();
    handleEnvelope(env, selfBuilt);
    handleUs_ += static_cast<uint64_t>(nowUs() - handleStart);
}

void Entity::handleEnvelope(const ProtocolEnvelope& env, bool selfBuilt) {
    if (inViewChange) return;
    ProtoMessage p(env);
    if (p.view() != currentView() || p.sequence() <= stableCheckpoint_ ||
        static_cast<int64_t>(p.sequence()) > static_cast<int64_t>(stableCheckpoint_) + consensusWindow() ||
        (!selfBuilt && !verifyEnvelope(env))) return;
    const auto phase = p.explicit_type();
    if (env.has_pre_prepare()) {
        const auto validateStart = nowUs();
        const bool valid = validateProposal(env, selfBuilt);
        validateUs_ += static_cast<uint64_t>(nowUs() - validateStart);
        if (!valid) return;
        auto existing = consensusInstances_.find(p.sequence());
        if (existing != consensusInstances_.end() && existing->second.proposal.has_pre_prepare() &&
            existing->second.proposal.digest() != env.digest()) return;
        auto committed = committedProofs_.find(p.sequence());
        if (committed != committedProofs_.end() && decodeEnvelope(committed->second["proposal"]).digest() != env.digest()) return;
        acceptProposal(env);
    } else if (env.has_prepare() && phase == "prepare") {
        if (env.prepare().combined_messages_size() || !env.prepare().operation().empty() || !env.prepare().qc().empty()) return;
        if (protocolName_ == "PBFT" && p.sender_id() == currentLeader()) return;
        if (protocolName_ == "SBFT" && !isCurrentLeader()) return;
        consensusInstances_[p.sequence()].prepares.emplace(p.sender_id(), env);
    } else if (env.has_commit()) {
        if (!env.commit().operation().empty() || !env.commit().qc().empty()) return;
        if (protocolName_ == "PBFT") {
            if (phase != "commit" || env.commit().combined_messages_size() || env.commit().fast_path()) return;
            consensusInstances_[p.sequence()].commits.emplace(p.sender_id(), env);
        } else if (phase == "commit") {
            if (!validateCertificate(env, "prepare", env.commit().fast_path() ? committeeSize() : 2 * f + 1)) return;
            auto& instance = consensusInstances_[p.sequence()];
            if (!instance.certificate.has_commit()) instance.certificate = env;
        } else if (phase == "commitAck") {
            if (!isCurrentLeader() || env.commit().combined_messages_size() || env.commit().fast_path()) return;
            consensusInstances_[p.sequence()].acks.emplace(p.sender_id(), env);
        } else if (phase == "execute") {
            if (env.commit().fast_path() || !validateCertificate(env, "commitAck", 2 * f + 1)) return;
            auto& instance = consensusInstances_[p.sequence()];
            if (!instance.execution.has_commit()) instance.execution = env;
        } else return;
    } else return;
    advanceConsensus(p.sequence());
}

void Entity::broadcastVote(ProtocolEnvelope env, bool collectorOnly) {
    signEnvelope(env);
    if (collectorOnly) {
        if (!isCurrentLeader()) sendProtocolTo(currentLeader(), env);
    } else sendProtocolToAll(env);
    // Local votes are logged before testing the corresponding quorum.
    auto& instance = consensusInstances_.at(ProtoMessage(env).sequence());
    if (env.has_prepare()) instance.prepares[nodeId] = env;
    else if (env.commit().type() == "commitAck") instance.acks[nodeId] = env;
    else instance.commits[nodeId] = env;
    if (protocolName_ == "SBFT" && env.has_prepare())
        fastVoteProofs_[env.prepare().sequence()] =
            json{{"proposal", encodeEnvelope(bedrock::strippedProposal(instance.proposal))}, {"vote", encodeEnvelope(env)}};
}

void Entity::rememberPrepared(int seq, const json& proof) {
    const int view = decodeEnvelope(proof["proposal"]).pre_prepare().view();
    auto it = preparedProofs_.find(seq);
    if (it == preparedProofs_.end() || decodeEnvelope(it->second["proposal"]).pre_prepare().view() <= view)
        preparedProofs_[seq] = proof;
    std::lock_guard<std::mutex> lock(phaseTsMtx);
    phaseTs_prepare.emplace(seq, nowUs());
}

void Entity::advanceConsensus(int seq) {
    auto& i = consensusInstances_.at(seq);
    if (!i.proposal.has_pre_prepare()) return;
    // PBFT accepts a pre-prepare only once the replica holds the request it
    // names. A new-view re-proposal arrives as a header; fetch its batch
    // before voting, so no replica ever votes for bytes it has not seen.
    if (!bedrock::carriesBody(i.proposal)) {
        attachLocalBody(i.proposal);
        if (!bedrock::carriesBody(i.proposal)) { requestBatch(seq); return; }
        batchIndex_[seq] = i.proposal.pre_prepare();
    }
    const int view = i.proposal.pre_prepare().view();
    const std::string digest = i.proposal.digest();
    if (!i.prepareSent && (protocolName_ == "SBFT" || !isCurrentLeader())) {
        i.prepareSent = true;
        broadcastVote(bedrock::voteEnvelope("prepare", view, seq, nodeId, digest), protocolName_ == "SBFT");
    }
    const auto prepares = matchingVotes(i.prepares, digest);
    if (protocolName_ == "PBFT") {
        if (prepares.size() >= static_cast<size_t>(2 * f) && !i.commitSent) {
            i.commitSent = true;
            rememberPrepared(seq, json{{"proposal", encodeEnvelope(bedrock::strippedProposal(i.proposal))}, {"prepares", prepares}});
            broadcastVote(bedrock::voteEnvelope("commit", view, seq, nodeId, digest), false);
        }
        const auto commits = matchingVotes(i.commits, digest);
        if (i.commitSent && commits.size() >= static_cast<size_t>(2 * f + 1) && !committedProofs_.count(seq)) {
            json proof = preparedProofs_.at(seq); proof["commits"] = commits;
            learnCommitted(proof, -1);
        }
        return;
    }
    if (isCurrentLeader() && !i.certificateSent) {
        if (prepares.size() == static_cast<size_t>(committeeSize())) {
            sbftDecide(seq, view, true);
            return; // deciding re-enters advanceConsensus
        }
        if (prepares.size() >= static_cast<size_t>(2 * f + 1) && !i.waitScheduled) {
            i.waitScheduled = true;
            scheduler_.scheduleAfter(std::chrono::milliseconds(std::max(1, fastPathWaitMs.load())), [this, seq, view] {
                std::lock_guard<std::recursive_mutex> lock(eventMtx);
                sbftDecide(seq, view, false);
            });
        }
    }
    if (i.certificate.has_commit() && i.certificate.digest() == digest) {
        const json proof{{"proposal", encodeEnvelope(bedrock::strippedProposal(i.proposal))},
                         {"certificate", encodeEnvelope(i.certificate)}};
        rememberPrepared(seq, proof);
        if (i.certificate.commit().fast_path()) {
            if (!committedProofs_.count(seq)) learnCommitted(proof, 1);
            return;
        }
        if (!i.ackSent) {
            i.ackSent = true;
            broadcastVote(bedrock::voteEnvelope("commitAck", view, seq, nodeId, digest), true);
        }
    }
    if (isCurrentLeader() && i.ackSent && !i.executeSent &&
        matchingVotes(i.acks, digest).size() >= static_cast<size_t>(2 * f + 1)) {
        i.executeSent = true;
        auto env = bedrock::voteEnvelope("execute", view, seq, nodeId, digest);
        for (const auto& [sender, vote] : i.acks)
            if (vote.digest() == digest) bedrock::appendCertificateVote(*env.mutable_commit(), vote);
        signEnvelope(env);
        i.execution = env;
        sendProtocolToAll(env);
    }
    if (i.execution.has_commit() && i.execution.digest() == digest && !committedProofs_.count(seq))
        learnCommitted(json{{"proposal", encodeEnvelope(bedrock::strippedProposal(i.proposal))},
                            {"certificate", encodeEnvelope(i.execution)}}, 0);
}

void Entity::sbftDecide(int seq, int view, bool fast) {
    if (inViewChange || currentView() != view || !isCurrentLeader()) return;
    auto found = consensusInstances_.find(seq);
    if (found == consensusInstances_.end()) return;
    auto& i = found->second;
    if (!i.proposal.has_pre_prepare() || i.proposal.pre_prepare().view() != view || i.certificateSent) return;
    const auto votes = matchingVotes(i.prepares, i.proposal.digest());
    if (votes.size() < static_cast<size_t>(fast ? committeeSize() : 2 * f + 1)) return;
    i.certificateSent = true;
    auto env = bedrock::voteEnvelope("commit", view, seq, nodeId, i.proposal.digest());
    env.mutable_commit()->set_fast_path(fast);
    for (const auto& [sender, vote] : i.prepares)
        if (vote.digest() == i.proposal.digest()) bedrock::appendCertificateVote(*env.mutable_commit(), vote);
    signEnvelope(env);
    i.certificate = env;
    sendProtocolToAll(env);
    advanceConsensus(seq);
}

void Entity::learnCommitted(const json& proof, int path) {
    const auto env = decodeEnvelope(proof.at("proposal"));
    const auto& p = env.pre_prepare();
    const int seq = p.sequence();
    if (seq <= stableCheckpoint_) return;
    auto previous = committedProofs_.find(seq);
    if (previous != committedProofs_.end() && decodeEnvelope(previous->second["proposal"]).digest() != env.digest())
        throw std::runtime_error("conflicting certified decisions");
    committedProofs_[seq] = proof;
    if (seq <= lastExecuted_) return;
    if (bedrock::carriesBody(env)) batchIndex_[seq] = p;
    {
        std::lock_guard<std::mutex> lock(phaseTsMtx);
        phaseTs_commit.emplace(seq, nowUs());
    }
    readySequences_[seq] = path;
    if (!inViewChange) requestTimer_.track("seq:" + std::to_string(seq));
    drainExecution();
}

void Entity::drainExecution() {
    if (drainingExecution_) return;
    drainingExecution_ = true;
    try {
        for (;;) {
            auto next = readySequences_.find(lastExecuted_ + 1);
            if (next == readySequences_.end()) break;
            const int seq = next->first, path = next->second;
            // A certified decision names its batch by digest. Execution
            // waits (and asks) for those bytes rather than failing.
            auto batch = batchIndex_.find(seq);
            if (batch == batchIndex_.end()) { requestBatch(seq); break; }
            // Only verified bodies are stored, so this holds; checking it
            // turns any future lapse into a stop rather than a silent
            // divergence from the rest of the committee. Proofs are headers,
            // so recovering the expected digest is cheap.
            if (bedrock::requestDigest(batch->second) != digestFor(seq))
                throw std::runtime_error("stored batch does not match the certified digest");
            if (!completeSequence(seq, path)) throw std::runtime_error("certified request missing at execution");
            readySequences_.erase(seq);
            lastExecuted_ = seq;
            lastConsensusProgress_ = Clock::now();
            if (seq % bedrock::kCheckpointInterval == 0) makeCheckpoint();
        }
    } catch (...) { drainingExecution_ = false; throw; }
    drainingExecution_ = false;
}
