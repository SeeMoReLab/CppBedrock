#pragma once

// One replica per process. PBFT/SBFT use an authenticated, explicit consensus
// engine (EntityConsensus.cpp), certified view changes (EntityViewChange.cpp),
// and checkpoint/state transfer (EntityRecovery.cpp). Their YAML files configure
// timers. Legacy protocols still use EventFactory's YAML actions.
// All protocol state changes run under engineMutex().

#include "events/EventHandler.h"
#include "events/BaseEvent.h"
#include "state/DataSet.h"
#include "state/EntityState.h"
#include "Committee.h"
#include "Consensus.h"
#include "ExecutedRequests.h"
#include "FailureSpec.h"
#include "PendingRequestTimer.h"
#include "TaskScheduler.h"
#include "TimeKeeper.h"
#include "agent/AgentClient.h"
#include "crypto/CryptoProvider.h"
#include "net/AsyncSender.h"
#include "net/ClientStreams.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include <yaml-cpp/yaml.h>
#include "proto/bedrock.grpc.pb.h"
#include "proto/bedrock.pb.h"

class NodeServiceImpl;
class Event;
class Message;

struct ProtocolMessageRecord {
    int seq;
    int senderId;
    std::string operation;
    std::string phase;         // e.g., "prepare", "commit"
    std::string protocolName;  // e.g., "Hotstuff"
    std::map<std::string, std::string> customData;
    ProtocolMessageRecord(int s, int sid, const std::string& op, const std::string& ph, const std::string& proto,
                         const std::map<std::string, std::string>& custom = {})
        : seq(s), senderId(sid), operation(op), phase(ph), protocolName(proto), customData(custom) {}
    ProtocolMessageRecord() = default;
};

// Store all view change data for each view as an array of structs
struct ViewChangeData {
    int sender;
    int last_sequence;
    std::string last_operation;
    nlohmann::json locked_qc;
};

struct EntityOptions {
    int nodeId{-1};
    std::string committeePath;
    std::string protocolConfigPath;
    std::string keysDir;
    // Learning agent; 0 disables the agent client.
    int agentPort{0};
    AgentClientConfig agentConfig;
    // Initial timeouts; 0 means "take the protocol config's timers value".
    int initialElectionTimeoutMs{0};
    int initialSlowPathTimeoutMs{0};
    // Shared failure_spec.xml; the replica reads only its protocol's section
    // (<pbft>, <sbft>, ...) for proposal-delay injection. Empty disables it.
    // failureStartUnixMs anchors the spec's phases (harness start_unix_ms).
    std::string failureSpecPath;
    std::uint64_t failureStartUnixMs{0};
    // Period of the "Stats" log line; 0 disables it.
    int statsIntervalMs{1000};
    // PBFT/SBFT require this to be true: all replica messages are signed.
    bool proposalSigning{true};
    int proposalIntervalMs{100};
    int batchMaxRequests{8192};
    int batchMaxBytes{512 * 1024};
    // Keeping the cadence at a 150 ms one-way delay needs roughly six batches
    // in flight, since a decision takes about four one-way trips. Evidence
    // names batches by digest, so the window costs kilobytes rather than
    // megabytes, and a run under added network delay needs it deep: 16
    // batches stall execution for seconds at a time at 150 ms one way,
    // because one late sequence blocks the rest and the leader then stops
    // proposing. The default stays modest so a large committee fits the
    // recovery message budget; raise it with --max-inflight-batches.
    int maxInflightBatches{bedrock::kDefaultConsensusWindow};
    int maxPendingRequests{32768};
    int maxPendingBytes{16 * 1024 * 1024};
    // Optional transport injection for deterministic protocol tests. The
    // callbacks enqueue delivery; they must not re-enter another replica.
    std::function<void(int, const bedrock::ProtocolEnvelope&)> protocolTransport;
    std::function<void(int, const std::string&)> controlTransport;
};

class Entity : public EventHandler<EntityState> {
    friend class Event;
public:
    using Clock = std::chrono::steady_clock;

    explicit Entity(const EntityOptions& options);
    ~Entity();

    void start();
    void stop();
    void handleEvent(const Event* event, EntityState* context) override;
    EntityState& getState();

    // ---- identity and committee ----
    int getNodeId() const { return nodeId; }
    int getF() const { return f; }
    int committeeSize() const { return committee_.size(); }
    const std::vector<int>& peerIds() const { return peerIds_; }
    int currentView() const { return entityInfo["view"].get<int>(); }
    int leaderForView(int view) const { return committee_.leaderForView(view); }
    int currentLeader() const { return leaderForView(currentView()); }
    bool isLeaderForView(int view) const { return leaderForView(view) == nodeId; }
    bool isCurrentLeader() const { return currentLeader() == nodeId; }
    const std::string& protocolName() const { return protocolName_; }
    bool proposalSigning() const { return options_.proposalSigning; }
    // True for the protocols that use the shared PBFT request timer, view
    // change, and request buffering (PBFT and SBFT).
    bool usesPbftCore() const { return pbftCore_; }

    // ---- replica-to-replica messaging ----
    void sendToAll(const Message& message);
    void sendTo(int peerId, const Message& message);
    void processProtocolEnvelope(const bedrock::ProtocolEnvelope& env);
    void sendProtocolToAll(const bedrock::ProtocolEnvelope& env);
    void sendProtocolTo(int peer, const bedrock::ProtocolEnvelope& env);
    // Accept a JSON message coming via gRPC and route it through the handler.
    bool processJsonFromGrpc(const std::string& json);
    // Serializes every state-machine transition; taken by handleEvent and by
    // the threads (request timer, scheduler) that mutate protocol state.
    // Recursive because handlers re-enter the engine (e.g. the leader
    // processes its own broadcast inline).
    std::recursive_mutex& engineMutex() { return eventMtx; }

    // ---- client replies ----
    bedrock::ClientStreams& clientStreams() { return clientStreams_; }
    void replyToClient(const std::string& clientId, uint64_t requestId, const std::string& result);

    // ---- protocol config ----
    void loadProtocolConfig(const std::string& configFile);
    YAML::Node getPhaseConfig(const std::string& phase) const;
    YAML::Node getPhaseConfigInsensitive(const std::string& phase) const;
    bool runVerification(const std::string& verifyType, const nlohmann::json& msg, EntityState* context);

    // ---- sequence bookkeeping ----
    int getNextSequenceNumber() { return nextSequenceNumber++; }
    int assignSequenceNumber() { return nextSequenceNumber + 1; }
    // Allocate a new monotonically increasing sequence (thread-safe, leader-side)
    int allocateNextSequence();
    void removeSequenceState(int seq);
    bool hasProcessedOperation(int seq) const;
    // Records when this replica first learned of the request behind seq
    // (client arrival on the leader, PrePrepare arrival on followers); the
    // consensus latency reported to the agent is measured from here.
    void noteFirstSeen(int seq, long long firstSeenUs);
    // Marks seq executed: feeds the learning agent sample (path: 1 fast,
    // 0 slow, -1 not applicable) and drives pruning.
    void markOperationProcessed(int seq, int path = -1, uint32_t transactions = 1, uint32_t batchSize = 1);
    static long long nowUs();
    // Aggregation key for a protocol phase: votes are counted per
    // (phase, view, sequence) so messages of an abandoned view never
    // contribute to a quorum in the next one.
    static std::string aggregationKey(const std::string& phase, int view, int seq);
    // Distinct senders recorded under aggregationKey(phase, view, seq).
    std::size_t senderCount(const std::string& phase, int view, int seq) const;
    std::set<int> senders(const std::string& phase, int view, int seq) const;

    // ---- request path (EntityRequests.cpp) ----
    struct PrePrepareInfo {
        std::string timestamp;
        std::string operation;
        std::string from;
        std::string to;
        int amount{0};
        std::string clientId;
        uint64_t requestId{0};
    };
    static std::string requestKey(const std::string& clientId, uint64_t requestId);
    // A client request arrived (any replica). Buffers it, arms the request
    // timer, and schedules the proposal when this replica leads.
    void onClientRequest(const nlohmann::json& request);
    // A PrePrepare for seq was stored: arms the request timer on followers
    // and records the request info used for execution and view changes.
    void onPrePrepareAccepted(int seq, int view);
    // Executes the request behind seq (once), replies to the client, and
    // releases every timer and buffer entry for it. path: 1 fast, 0 slow,
    // -1 not applicable. Returns false when the PrePrepare is unknown.
    bool completeSequence(int seq, int path);
    std::size_t pendingRequestCount() const;
    // Thread-safe bounded ingress, bypassing the consensus RPC work queue.
    void submitClientRequest(const bedrock::ClientRequest& request);
    // A single cadence opportunity, also driven explicitly by protocol tests.
    void proposalTick();

    // ---- view changes (EntityViewChange.cpp) ----
    void onRequestTimerExpired(std::uint64_t generation);
    void startViewChange(int newView, const std::string& reason);
    void onViewChangeMessage(const nlohmann::json& msg);
    void onNewViewMessage(const nlohmann::json& msg);
    // Applies a recommendation from the learning agent (learning thread).
    void applyAgentTimeouts(const AgentTimeouts& timeouts);

    // ---- legacy view change (Hotstuff/Zyzzyva paths) ----
    void onTimeout();
    void sendNewViewToNextLeader();
    void initiateViewChange();

    // SBFT timeout callback checks both the view and sequence.
    void sbftDecide(int seq, int view, bool fast);
    bedrock::TaskScheduler& scheduler() { return scheduler_; }
    int executedThrough() const { return lastExecuted_; }
    int stableCheckpoint() const { return stableCheckpoint_; }
    // Periodic retransmission and catch-up, also driven explicitly in tests.
    void maintainConsensus();

    // ---- balances (smallbank-style transfers) ----
    void updateBalances(const std::string& from, const std::string& to, int amount) {
        std::lock_guard<std::mutex> lock(balancesMutex);
        applyTransfer(from, to, amount);
    }

    // Applies one transfer with balancesMutex already held, so executing a
    // batch takes the lock once rather than once per transaction.
    void applyTransfer(const std::string& from, const std::string& to, int amount) {
        if (balances.find(from) == balances.end()) balances[from] = 100;
        if (balances.find(to) == balances.end()) balances[to] = 100;
        balances[from] -= amount;
        balances[to] += amount;
    }

    void updateSpeculativeBalances(const std::string& from, const std::string& to, int amount) {
        std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
        if (speculativeBalances.find(from) == speculativeBalances.end()) speculativeBalances[from] = 100;
        if (speculativeBalances.find(to) == speculativeBalances.end()) speculativeBalances[to] = 100;
        speculativeBalances[from] -= amount;
        speculativeBalances[to] += amount;
    }

    int getSpeculativeBalance(const std::string& account) {
        std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
        auto it = speculativeBalances.find(account);
        return (it != speculativeBalances.end()) ? it->second : 100;
    }

    void markSpeculativeTransactionExecuted(const std::string& txnId) {
        executedSpeculativeTransactions.insert(txnId);
    }

    bool hasExecutedSpeculativeTransaction(const std::string& txnId) const {
        return executedSpeculativeTransactions.find(txnId) != executedSpeculativeTransactions.end();
    }

    void clearSpeculativeBalances() {
        std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
        speculativeBalances.clear();
        executedSpeculativeTransactions.clear();
    }

    struct SpeculativeEntry {
        int seq;
        std::string txnId;
        std::string from;
        std::string to;
        int amount;
    };

    void appendSpeculativeEntry(int seq, const std::string& txnId,
                                const std::string& from, const std::string& to, int amount) {
        std::lock_guard<std::mutex> g(speculativeLogMtx);
        if (speculativeLog.find(seq) == speculativeLog.end()) {
            speculativeLog.emplace(seq, SpeculativeEntry{seq, txnId, from, to, amount});
        }
    }

    int findSeqByTxnId(const std::string& txnId) {
        std::lock_guard<std::mutex> g(speculativeLogMtx);
        for (const auto& [k, e] : speculativeLog) {
            if (e.txnId == txnId) return e.seq;
        }
        return -1;
    }

    void rebuildSpeculativeBalancesFromLog() {
        {
            std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
            speculativeBalances.clear();
        }
        std::lock_guard<std::mutex> g(speculativeLogMtx);
        for (const auto& [k, e] : speculativeLog) {
            if (e.seq > committedSeq) {
                updateSpeculativeBalances(e.from, e.to, e.amount);
            }
        }
    }

    // ---- protocol state shared with the event implementations ----
    std::map<std::string, int> balances;
    std::mutex balancesMutex;
    std::map<int, EntityState> sequenceStates;
    std::unordered_map<int, std::set<int>> prePrepareMessages;
    std::unordered_map<int, std::set<int>> prepareMessages;
    std::unordered_map<int, std::set<int>> commitMessages;
    std::unordered_map<int, std::string> prePrepareOperations;
    std::unordered_map<int, std::string> prepareOperations;
    std::unordered_map<int, std::string> commitOperations;
    // Legacy (Hotstuff/Zyzzyva) view-change buffers.
    std::unordered_map<int, std::vector<nlohmann::json>> viewChangeMessages;
    bool inViewChange = false;
    std::unique_ptr<TimeKeeper> timeKeeper;
    std::mutex timerMtx;

    std::unordered_map<int, std::unordered_map<std::string, std::set<int>>> receivedMessages;
    std::set<int> processedOperations;
    std::unordered_map<int, std::atomic<bool>> preparePhaseTimerRunning;
    std::unordered_map<std::string, std::unique_ptr<BaseEvent>> actions;

    std::map<int, std::vector<ProtocolMessageRecord>> allMessagesBySeq;
    std::unordered_map<int, std::vector<ViewChangeData>> viewChangeDataArray;

    DataSet dataset;
    nlohmann::json entityInfo;
    std::unique_ptr<CryptoProvider> cryptoProvider;
    YAML::Node protocolConfig;
    // Timeouts read by the timers at every arm; written by the learning agent.
    std::atomic<int> viewChangeTimeoutMs{8000};
    std::atomic<int> fastPathWaitMs{20};

    mutable std::mutex senderIdsMtx;
    std::unordered_map<std::string, std::unordered_set<int>> keyToSenderIds;

    // Quorum deduplication: ensures broadcast fires exactly once per phase+seq
    std::mutex quorumTriggeredMtx;
    std::unordered_set<std::string> quorumTriggered;

    std::mutex prePrepareMtx;

    std::map<int, std::set<int>> newViewHotstuffSenders;
    nlohmann::json piggyback;
    std::mutex piggybackMtx;
    bool piggybackBroadcastStarted = false;

    std::map<std::string, int> speculativeBalances;
    std::mutex speculativeBalancesMutex;
    std::unordered_set<std::string> executedSpeculativeTransactions;
    std::map<int, SpeculativeEntry> speculativeLog;
    mutable std::mutex speculativeLogMtx;
    int committedSeq = 0;
    int f;

    void sendFillHole(int fromSeq, int toSeq, bool broadcast);
    void tryHandleFillHoleTimeout();
    int getMaxSpeculativeSeq() const;
    void cachePrePrepare(int seq, const nlohmann::json& msg);
    void replayRangeTo(int fromSeq, int toSeq, int targetNodeId);

    // Fast lookup for execution and view changes (seq -> request info from the PrePrepare)
    std::unordered_map<int, PrePrepareInfo> prePrepareIndex;

    mutable std::mutex processedMtx;     // protects processedOperations

    // Phase timestamps (microseconds, steady clock), keyed by sequence.
    std::unordered_map<int, long long> phaseTs_preprepare; // seq -> PrePrepare stored
    std::unordered_map<int, long long> phaseTs_prepare;    // seq -> prepared (quorum or certificate)
    std::unordered_map<int, long long> phaseTs_commit;     // seq -> committed (quorum or certificate)
    std::unordered_map<int, long long> firstSeenUs;        // seq -> request first seen locally
    std::mutex phaseTsMtx;

private:
    friend struct EntityTimerTestAccess;

    struct PendingRequest {
        nlohmann::json request;
        std::string clientId;
        uint64_t requestId{0};
        long long arrivalUs{0};
        Clock::time_point acceptedAt{};
        int proposedInView{-1};   // -1: not proposed by this replica yet
        bedrock::ClientRequest wire;
        size_t bytes{0};
    };

    EntityOptions options_;
    int nodeId;
    bedrock::Committee committee_;
    std::vector<int> peerIds_;
    std::string protocolName_;
    bool pbftCore_{false};

    EntityState _entityState;

    std::atomic<int> nextSequenceNumber{0};
    std::mutex clientRequestMtx;            // serialize leader request handling

    std::atomic<bool> running = false;

    std::map<int, nlohmann::json> preprepareCache;
    std::atomic<bool> fillHolePending{false};
    int fillHoleFromSeq{0};
    int fillHoleToSeq{0};
    std::chrono::steady_clock::time_point fillHoleDeadline;
    int fillHoleTimeoutMs{600};

    // Serializes message handling (see engineMutex()).
    std::recursive_mutex eventMtx;

    // gRPC server hosted by this replica
    std::unique_ptr<NodeServiceImpl> grpcSvc_;
    std::unique_ptr<grpc::Server> grpcServer_;
    std::thread grpcThread_;
    void startGrpcServer();
    void stopGrpcServer();

    bedrock::AsyncSender sender_;
    bedrock::ClientStreams clientStreams_;

    // Learning agent (optional, per-node).
    std::unique_ptr<AgentClient> agentClient_;

    // Periodic stats line.
    std::thread statsThread_;
    std::mutex statsMtx_;
    std::condition_variable statsCv_;
    void statsLoop();
    std::atomic<uint64_t> committedTotal_{0};
    std::atomic<uint64_t> committedTransactions_{0};
    std::atomic<uint64_t> rejectedRequests_{0};
    std::atomic<uint64_t> requestsReceived_{0};
    std::atomic<uint64_t> repliesSent_{0};
    std::atomic<uint64_t> viewChangesStarted_{0};
    std::atomic<uint64_t> newViewsInstalled_{0};
    std::atomic<uint64_t> fastPathTotal_{0};
    std::atomic<uint64_t> slowPathTotal_{0};
    std::atomic<uint64_t> delayedProposals_{0};
    // Relaying is how a request a backup holds but the leader does not reaches
    // the leader. If a watched request starves while these stay flat, the
    // watchdog is suspecting a leader that was never given the request.
    std::atomic<uint64_t> relaysSent_{0};      // relay batches this replica sent to the leader
    std::atomic<uint64_t> relaysAccepted_{0};  // relay batches this replica accepted as leader
    std::atomic<uint64_t> relayedRequests_{0}; // requests taken from relays into a proposal
    // Engine CPU accounting, reported as per-second microseconds in Stats.
    // Consensus batches amortize messages, not per-request work, so this is
    // where a batching throughput ceiling becomes visible.
    std::atomic<uint64_t> proposeUs_{0};   // building, digesting and signing proposals
    std::atomic<uint64_t> validateUs_{0};  // verifying incoming proposals
    std::atomic<uint64_t> executeUs_{0};   // executing batches and replying to clients
    std::atomic<uint64_t> admitUs_{0};     // admitting client requests into the pending pool
    std::atomic<uint64_t> assembleUs_{0};  // selecting requests into the next batch
    std::atomic<uint64_t> handleUs_{0};    // all consensus message handling, including the two above
    std::atomic<uint64_t> tickWaitUs_{0};  // proposal ticks waiting for the engine lock
    std::atomic<uint64_t> proposalTicks_{0};   // cadence opportunities that fired
    std::atomic<uint64_t> batchesProposed_{0}; // ticks that produced a batch

    // Sequence-state pruning: everything below pruneFloor_ has been released.
    int highestCommittedSeq_{0};
    int pruneFloor_{0};
    void pruneSequenceState();

    // ---- PBFT core state (all under eventMtx) ----
    // Sequences the log and view-change evidence may span above the stable
    // checkpoint. It never falls below the default, so a run that keeps only
    // a batch or two in flight still has room for the checkpoint interval,
    // and it follows --max-inflight-batches upward so a deeper pipeline is
    // not clipped by the log. Every replica in a committee must derive the
    // same value, since it bounds the evidence each will accept.
    int consensusWindow() const {
        return std::max(options_.maxInflightBatches, bedrock::kDefaultConsensusWindow);
    }

    bedrock::PendingRequestTimer requestTimer_;
    bedrock::TaskScheduler scheduler_;
    std::optional<Clock::time_point> viewChangeWaitSince_;
    uint64_t viewChangeWaitGeneration_{0};
    unsigned viewChangeBackoff_{0};
    std::chrono::milliseconds viewChangeWaitDuration() const;
    void armViewChangeWait();
    void cancelViewChangeWait();
    void scheduleViewChangeWait();
    void onViewChangeWaitExpired(uint64_t generation);
    std::unique_ptr<bedrock::ProposalDelayController> proposalDelay_;
    // Hashed, not ordered: the pool holds tens of thousands of entries under
    // load and nothing iterates it in key order.
    std::unordered_map<std::string, PendingRequest> pendingRequests_;  // key -> buffered client request
    std::deque<std::string> proposalQueue_;
    size_t pendingBytes_{0};
    // One bounded relay batch per peer, separate from saturated client ingress.
    std::map<int, std::deque<bedrock::ClientRequest>> forwardedRequests_;
    int nextProposalSource_{0};
    Clock::time_point lastRequestForward_{};
    std::string lastForwardedKey_;
    void forwardPendingRequests();
    void acceptForwardedRequests(const nlohmann::json& message);
    std::mutex ingressMtx_;
    std::deque<std::pair<bedrock::ClientRequest, long long>> ingress_;
    size_t ingressBytes_{0};
    Clock::time_point nextProposalTick_{};
    uint64_t proposalGeneration_{0};
    Clock::time_point lastRecoveryRequest_{};
    Clock::time_point lastConsensusProgress_{Clock::now()};
    // Batch bodies this replica holds, by sequence. Consensus evidence
    // names batches by digest; this is where the named bytes live.
    std::map<int, bedrock::PrePrepare> batchIndex_;
    std::map<int, Clock::time_point> lastBatchRequest_;
    bedrock::ExecutedRequests executedRequests_;           // request keys executed locally
    std::map<int, std::string> executedRequestBySeq_;            // for pruning executedRequests_
    std::map<int, std::map<int, nlohmann::json>> viewChangeMsgs_; // view -> sender -> ViewChange
    int lastNewViewSent_{-1};

    // Drops queued delayed proposals (view moved on) and keeps the periodic
    // leader observation of the failure spec alive.
    void cancelDelayedProposals();
    void scheduleLeaderObservation();
    void scheduleProposalTick();
    void proposeBatch();
    void acceptClientRequest(const bedrock::ClientRequest& request, long long arrival);
    void rebuildProposalQueue();
    void scheduleProposal(const std::string& key);
    void proposeRequest(const std::string& key);
    void proposeBufferedRequests();
    void tryBuildNewView(int view);
    void installReProposal(const nlohmann::json& prePrepare, int view);

    // Maintained PBFT/SBFT engine. All state is protected by eventMtx.
    std::string consensusDomain_;
    std::map<int, bedrock::ConsensusInstance> consensusInstances_;
    std::map<int, nlohmann::json> preparedProofs_;
    std::map<int, nlohmann::json> fastVoteProofs_;
    std::map<int, nlohmann::json> committedProofs_;
    std::map<int, int> readySequences_;
    int lastExecuted_{0};
    int stableCheckpoint_{0};
    nlohmann::json stableCheckpointProof_;
    nlohmann::json stableSnapshot_;
    std::map<int, nlohmann::json> checkpointSnapshots_;
    std::map<int, std::map<std::string, std::map<int, nlohmann::json>>> checkpointVotes_;
    nlohmann::json pendingNewView_;
    nlohmann::json lastNewView_;
    bool drainingExecution_{false};

    void initializeConsensus();
    void signEnvelope(bedrock::ProtocolEnvelope& env);
    bool verifyEnvelope(const bedrock::ProtocolEnvelope& env);
    nlohmann::json signControl(nlohmann::json msg);
    bool verifyControl(const nlohmann::json& msg);
    // Accepts a proposal header, and its batch when one is attached.
    // Evidence carries headers; voting and execution additionally require
    // the batch the header's digest names.
    bool validateProposal(const bedrock::ProtocolEnvelope& env, bool selfBuilt = false);
    // The digest this replica is bound to for seq, from its own evidence.
    std::string digestFor(int seq) const;
    // Fills env's batch from a locally held copy when the digest matches.
    void attachLocalBody(bedrock::ProtocolEnvelope& env) const;
    // Asks peers for the batch behind an accepted or certified sequence.
    void requestBatch(int seq);
    // Installs a body that hashes to the digest seq is bound to.
    void installBatchBody(int seq, const bedrock::PrePrepare& body);
    bool validateCertificate(const bedrock::ProtocolEnvelope& env, const std::string& phase, int quorum);
    bool validatePreparedProof(const nlohmann::json& proof);
    bool validateCommittedProof(const nlohmann::json& proof);
    bool validateCheckpoint(const nlohmann::json& proof);
    bool validateViewChange(const nlohmann::json& msg);
    nlohmann::json selectNewView(const nlohmann::json& changes, int view);
    // selfBuilt marks the one envelope a replica did not receive: the proposal
    // the leader assembled, digested and signed microseconds earlier in
    // proposeBatch. Re-deriving a digest over half a megabyte it just produced,
    // and verifying its own signature, cost 87 ms of every second on the thread
    // that sets the proposal cadence. Every structural check still runs; only
    // the two that re-derive what this replica computed are skipped.
    void handleConsensusEnvelope(const bedrock::ProtocolEnvelope& env, bool selfBuilt = false);
    void handleEnvelope(const bedrock::ProtocolEnvelope& env, bool selfBuilt);
    void handleConsensusControl(const nlohmann::json& msg);
    void advanceConsensus(int seq);
    void acceptProposal(const bedrock::ProtocolEnvelope& env);
    void broadcastVote(bedrock::ProtocolEnvelope env, bool collectorOnly);
    void rememberPrepared(int seq, const nlohmann::json& proof);
    void learnCommitted(const nlohmann::json& proof, int path);
    void drainExecution();
    void finishNewView(const nlohmann::json& msg);
    void makeCheckpoint();
    void acceptCheckpoint(const nlohmann::json& msg);
    void stabilizeCheckpoint(const nlohmann::json& proof);
    void pruneCertifiedPrefix();
    void requestRecovery();
    void installRecovery(const nlohmann::json& msg);
    void scheduleConsensusMaintenance();
};
