#pragma once

#include "proto/bedrock.pb.h"
#include <map>
#include <string>
#include <nlohmann/json.hpp>

namespace bedrock {

// The sequence window is advanced only by certified checkpoints.
// Batches are much larger than individual requests. Bound the retained
// evidence window as well as the number of unexecuted proposals.
constexpr int kCheckpointInterval = 2;
// Sequences the log may hold above the stable checkpoint, and so the most
// batches that can be in flight. Evidence names batches by digest, so this
// window costs kilobytes of view-change evidence rather than megabytes, and
// no longer has to be kept tiny. The window a run actually uses is
// --max-inflight-batches: it bounds the log, the in-flight proposals and the
// evidence every replica will accept, so every replica in a committee must
// be given the same value. The ceiling here only bounds what may be asked
// for; a committee large enough to exceed the recovery message budget at the
// requested window is rejected at startup.
constexpr int kMaxConsensusWindow = 256;
constexpr int kDefaultConsensusWindow = 16;
// How often a replica checks for stalled sequences and retransmits. It also
// bounds how long execution may stall before retransmission starts.
constexpr int kMaintenanceIntervalMs = 250;
constexpr int kMaxBatchRequests = 8192;
constexpr int kMaxBatchBytes = 512 * 1024;

// One instance in one view. Cleared on view installation; evidence needed
// across views lives separately in the prepared/committed proof logs.
struct ConsensusInstance {
    ProtocolEnvelope proposal;
    std::map<int, ProtocolEnvelope> prepares;
    std::map<int, ProtocolEnvelope> commits;
    std::map<int, ProtocolEnvelope> acks;
    ProtocolEnvelope certificate;
    ProtocolEnvelope execution;
    bool prepareSent{false};
    bool commitSent{false};
    bool ackSent{false};
    bool waitScheduled{false};
    bool certificateSent{false};
    bool executeSent{false};
};

// Proofs embed the original signed protobuf bytes, not reconstructed claims
// about signers. Hex encoding keeps the JSON control channel unambiguous.
//
// A replica signature covers the proposal header only (view, sequence,
// leader, batch digest), never the batch body. Canonical PBFT keeps request
// bodies out of the protocol messages: consensus evidence names a batch by
// its digest, and a body is accepted from any source that hashes to it.
// Evidence therefore stays kilobytes wide however large the batch is.
std::string encodeEnvelope(const ProtocolEnvelope& envelope);
ProtocolEnvelope decodeEnvelope(const nlohmann::json& encoded);
std::string requestDigest(const PrePrepare& proposal);
// The bytes a replica signature covers: the envelope without its signature
// and without the batch body.
std::string signedHeaderBytes(const ProtocolEnvelope& envelope);
// The same envelope carrying only its header, for embedding in evidence.
ProtocolEnvelope strippedProposal(const ProtocolEnvelope& envelope);
// The digest of an empty batch. Every replica can compute it without holding
// any request bytes, so it is the one digest that names its own body.
const std::string& emptyBatchDigest();
// True when the envelope carries the batch its digest names, established by
// recomputing the digest over the body. Linear in the batch, so this belongs
// at the trust boundary only: validating an incoming proposal, and installing
// a fetched batch.
bool bodyMatchesDigest(const ProtocolEnvelope& envelope);
// True when the envelope carries its batch, for an envelope that has already
// passed validateProposal. Validation admits a proposal only when it is
// header-only with an empty request list, or carries a body whose digest
// matches, so a non-empty request list is exactly a verified body, and an
// empty batch is recognised by its digest. Constant time, for the hot path.
bool carriesBody(const ProtocolEnvelope& envelope);
ProtocolEnvelope voteEnvelope(const std::string& phase, int view, int seq,
                              int sender, const std::string& digest);
ProtocolEnvelope certificateVote(const AggregatedMessage& share, const std::string& phase);
void appendCertificateVote(Commit& certificate, const ProtocolEnvelope& vote);

} // namespace bedrock
