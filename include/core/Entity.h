#pragma once

#include "events/EventHandler.h"
#include "events/BaseEvent.h"
#include "state/StateMachine.h"
#include "state/DataSet.h"
#include "state/EntityState.h"
#include "pipeline/Pipeline.h"
#include "../coordination/connections/TcpConnection.h"
#include <vector>
#include <memory>
#include <yaml-cpp/yaml.h>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <nlohmann/json.hpp>
#include <map>
#include "TimeKeeper.h"
#include <atomic>
#include "crypto/CryptoProvider.h"
#include "crypto/OpenSSLCryptoProvider.h"
#include "crypto/CryptoUtils.h"
#include "utils/Benchmark.h"
#include <string>
#include <mutex>
#include <iostream>
#include <memory>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "proto/bedrock.grpc.pb.h"
#include "proto/bedrock.pb.h"
#include "proto/agent.grpc.pb.h"
#include "proto/agent.pb.h"
#include <functional> // For std::hash

namespace grpc { class Server; }
class NodeServiceImpl;

class Event;
class Message;

struct ProtocolMessageRecord {
    int seq;
    int senderId;
    std::string operation;
    std::string phase;         // e.g., "prepare", "commit"
    std::string protocolName;  // e.g., "Hotstuff"
    std::map<std::string, std::string> customData; // default: empty
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

// Custom hash function for std::pair
struct PairHash {
    template <typename T1, typename T2>
    std::size_t operator()(const std::pair<T1, T2>& pair) const {
        return std::hash<T1>()(pair.first) ^ (std::hash<T2>()(pair.second) << 1);
    }
};

class Entity : public EventHandler<EntityState> {
    friend class Event; // <-- Add this line
public:
    Entity(const std::string& role, int id, const std::vector<int>& peers, bool byzantine = false);
    ~Entity(); // <-- Add this line
    void start(); // ensure you call startGrpcServer() inside
    void stop();  // ensure you call stopGrpcServer() inside
    void handleEvent(const Event* event, EntityState* context) override;
    EntityState& getState();
    void sendToAll(const Message& message);
    void sendTo(int peerId, const Message& message);
    void processMessages();
    void loadProtocolConfig(const std::string& configFile);

    int getNodeId() const { return nodeId; }
    int getF() const { return f; }
    int getView() const { return _entityState.getView(); }
    void storePrepareMessage(int nodeId, int sequence);
    void storeCommitMessage(int nodeId, int sequence);
    void storePrePrepareMessage(int nodeId, int sequence);
    void storePrePrepareMessage(int nodeId, int sequence, const std::string& operation);
    void storePrepareMessage(int nodeId, int sequence, const std::string& operation);
    void storeCommitMessage(int nodeId, int sequence, const std::string& operation);
    int getPrepareCount(int sequence) const;
    int getCommitCount(int sequence) const;
    int getPrePrepareCount(int sequence) const;
    void printDataStore();
    void printCommittedMessages();
    void loadOrInitDataset();
    void initiateViewChange();
    void updateEntityInfoField(const std::string& key, const nlohmann::json& value);
    
    void saveEntityInfo();
    // Protocol config access
    //const YAML::Node& getPhaseConfig(const std::string& phase) const;
    YAML::Node getPhaseConfig(const std::string& phase) const;
    YAML::Node getPhaseConfigInsensitive(const std::string& phase) const;

    // Protocol-agnostic verification
    bool runVerification(const std::string& verifyType, const nlohmann::json& msg, EntityState* context);

    int getNextSequenceNumber() { return nextSequenceNumber++; }
    int assignSequenceNumber() {
        return nextSequenceNumber+1;
    }

    void removeSequenceState(int seq);

    const std::unordered_map<int, std::string>& getPrePrepareOperations() const { return prePrepareOperations; }

    std::set<int> prePrepareBroadcasted;

    bool hasProcessedOperation(const int operation) const {
        return processedOperations.find(operation) != processedOperations.end();
    }
    
    void markOperationProcessed(const int operation);

    void updateBalances(const std::string& from, const std::string& to, int amount) {
        std::lock_guard<std::mutex> lock(balancesMutex);
        if (balances.find(from) == balances.end()) balances[from] = 100;
        if (balances.find(to) == balances.end()) balances[to] = 100;
        balances[from] -= amount;
        balances[to] += amount;
        // std::cout << "[Node " << getNodeId() << "] Updated balances: " << from << "=" << balances[from] << ", " << to << "=" << balances[to] << std::endl;
    }

    // Update speculative balances (thread-safe)
    void updateSpeculativeBalances(const std::string& from, const std::string& to, int amount) {
        std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
        if (speculativeBalances.find(from) == speculativeBalances.end()) speculativeBalances[from] = 100;
        if (speculativeBalances.find(to) == speculativeBalances.end()) speculativeBalances[to] = 100;
        speculativeBalances[from] -= amount;
        speculativeBalances[to] += amount;
        std::cout << "[Node " << getNodeId() << "] Updated SPECULATIVE balances: " << from << "=" << speculativeBalances[from] << ", " << to << "=" << speculativeBalances[to] << std::endl;
    }

    // Get speculative balance for an account (thread-safe)
    int getSpeculativeBalance(const std::string& account) {
        std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
        auto it = speculativeBalances.find(account);
        return (it != speculativeBalances.end()) ? it->second : 100;
    }

    // Mark a speculative transaction as executed
    void markSpeculativeTransactionExecuted(const std::string& txnId) {
        executedSpeculativeTransactions.insert(txnId);
    }

    // Check if a speculative transaction has been executed
    bool hasExecutedSpeculativeTransaction(const std::string& txnId) const {
        return executedSpeculativeTransactions.find(txnId) != executedSpeculativeTransactions.end();
    }

    // Optionally, clear all speculative balances (e.g., on commit/rollback)
    void clearSpeculativeBalances() {
        std::lock_guard<std::mutex> lock(speculativeBalancesMutex);
        speculativeBalances.clear();
        executedSpeculativeTransactions.clear();
    }

    // Speculative log entry
    struct SpeculativeEntry {
        int seq;
        std::string txnId;
        std::string from;
        std::string to;
        int amount;
    };

    // Append to speculative log
    void appendSpeculativeEntry(int seq, const std::string& txnId,
                                const std::string& from, const std::string& to, int amount) {
        std::lock_guard<std::mutex> g(speculativeLogMtx);
        if (speculativeLog.find(seq) == speculativeLog.end()) {
            speculativeLog.emplace(seq, SpeculativeEntry{seq, txnId, from, to, amount});
        }
    }

    // Find seq by txnId (timestamp). Returns -1 if not found.
    int findSeqByTxnId(const std::string& txnId) {
        std::lock_guard<std::mutex> g(speculativeLogMtx);
        for (const auto& [k, e] : speculativeLog) {
            if (e.txnId == txnId) return e.seq;
        }
        return -1;
    }

    // Rebuild speculative balances from current speculative log > committedSeq
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

    std::vector<int> peerPorts;
    std::map<std::string, int> balances;
    std::mutex balancesMutex;
    std::map<int, EntityState> sequenceStates;
    std::unordered_map<int, std::set<int>> prePrepareMessages;
    std::unordered_map<int, std::set<int>> prepareMessages;
    std::unordered_map<int, std::set<int>> commitMessages;
    std::unordered_map<int, std::string> prePrepareOperations;
    std::unordered_map<int, std::string> prepareOperations;
    std::unordered_map<int, std::string> commitOperations;
    std::unordered_map<int, std::vector<nlohmann::json>> viewChangeMessages;
    std::unordered_map<int, nlohmann::json> latestPreparePerSeq;
    std::mutex latestPrepareMtx;
    std::unordered_map<std::pair<int, int>, int, PairHash> nodeDelays;
    bool inViewChange = false;
    std::unique_ptr<TimeKeeper> timeKeeper;
    std::mutex timerMtx;

    // Track received messages: [sequence][type][sender]
    std::unordered_map<int, std::unordered_map<std::string, std::set<int>>> receivedMessages;
    std::set<int> processedOperations;  // Track completed operations
    std::unordered_map<int, std::atomic<bool>> preparePhaseTimerRunning;
    std::unordered_map<std::string, std::unique_ptr<BaseEvent>> actions;

    std::map<int, std::vector<ProtocolMessageRecord>> allMessagesBySeq;
    std::unordered_map<int, std::vector<ViewChangeData>> viewChangeDataArray;

    DataSet dataset;
    nlohmann::json entityInfo;
    std::unique_ptr<CryptoProvider> cryptoProvider;
    YAML::Node protocolConfig;
    int viewChangeTimeoutMs{8000};
    int fastPathWaitMs{20};
    void onTimeout();
    void sendNewViewToNextLeader();

    bool isByzantine;
    std::unordered_map<int, std::unique_ptr<TimeKeeper>> prepareTimers;

    std::mutex senderIdsMtx;
    std::unordered_map<std::string, std::unordered_set<int>> keyToSenderIds;

    // Quorum deduplication: ensures broadcast fires exactly once per phase+seq
    std::mutex quorumTriggeredMtx;
    std::unordered_set<std::string> quorumTriggered;

    std::mutex prePrepareMtx;

    std::unordered_set<std::string> executedTransactions;

    std::map<int, std::set<int>> newViewHotstuffSenders;
    nlohmann::json piggyback; // Stores the latest piggyback info
    std::mutex piggybackMtx;
    bool piggybackBroadcastStarted = false;

    // For Zyzzyva and speculative execution
    std::map<std::string, int> speculativeBalances;      // Speculative balances for speculative complete
    std::mutex speculativeBalancesMutex;                 // Mutex for thread-safe access to speculativeBalances

    // For tracking speculative transactions (optional, similar to executedTransactions)
    std::unordered_set<std::string> executedSpeculativeTransactions;

    // New: speculative log and commit index
    std::map<int, SpeculativeEntry> speculativeLog; // key: seq
    mutable std::mutex speculativeLogMtx; // CHANGED: added mutable
    int committedSeq = 0;
    int f;

    void sendFillHole(int fromSeq, int toSeq, bool broadcast);
    void tryHandleFillHoleTimeout();
    int getMaxSpeculativeSeq() const;
    void cachePrePrepare(int seq, const nlohmann::json& msg);
    void replayRangeTo(int fromSeq, int toSeq, int targetNodeId);

    // Allocate a new monotonically increasing sequence (thread-safe, leader-side)
    int allocateNextSequence();

    // Accept a JSON message coming via gRPC and route it through the normal handler
    bool processJsonFromGrpc(const std::string& json);

    // Protobuf-only basic phases
    void processProtocolEnvelope(const bedrock::ProtocolEnvelope& env);
    void sendProtocolToAll(const bedrock::ProtocolEnvelope& env);
    void sendProtocolTo(int peer, const bedrock::ProtocolEnvelope& env);
    void loadDelaysFromConfig(const std::string& configFile);

    struct PrePrepareInfo {
        std::string timestamp;
        std::string operation;
        std::string from;
        std::string to;
        int amount{0};
        int client_port{-1};
    };

    // Fast lookup for CompleteEvent; does not change existing dataset behavior
    std::unordered_map<int, PrePrepareInfo> prePrepareIndex; // seq -> info

    
    mutable std::mutex processedMtx;     // protects processedOperations

    // ---- Byzantine fault-injection schedule ----
    struct ByzantineNodeState {
        int proposalDelayMs{0};
        bool skipFastPath{false};
        int fastPathExtraDelayMs{0};
    };
    struct ByzantineScheduleEntry {
        int atSeconds{0};
        std::unordered_map<int, ByzantineNodeState> nodes; // nodeId -> state
    };
    std::vector<ByzantineScheduleEntry> byzantineSchedule; // sorted ascending by atSeconds
    std::chrono::steady_clock::time_point byzantineStartTime;

    // Cached active state — updated lazily when the next entry's time has elapsed.
    // Entries that list this node fully replace the cached state (omitted fields → default).
    // Entries that don't mention this node leave the cached state unchanged.
    mutable std::mutex byzantineMtx;
    mutable size_t nextByzScheduleIdx{0};
    mutable ByzantineNodeState activeByzantineState;

    void loadByzantineSchedule(const std::string& configFile);
    ByzantineNodeState getActiveByzantineState() const;

    // Windowed metrics (also protected by processedMtx)
    Benchmark nodeBench;
    Benchmark phaseBench_preprepare{"preprepare_phase"};
    Benchmark phaseBench_prepare{"prepare_phase"};
    Benchmark phaseBench_commit{"commit_phase"};
    int windowSize = 100;
    int windowTxCount = 0;
    int windowId = 1;

    std::unordered_map<int, long long> phaseTs_preprepare; // seq → µs when PrePrepare stored
    std::unordered_map<int, long long> phaseTs_prepare;    // seq → µs when prepare quorum first met
    std::unordered_map<int, long long> phaseTs_commit;     // seq → µs when commit quorum first met
    std::mutex phaseTsMtx;

private:
    int nodeId;
    
    EntityState _entityState;
    TcpConnection connection;
    std::thread processingThread;

    
    
    
    std::atomic<int> nextSequenceNumber{0}; // was plain int
    std::mutex clientRequestMtx;            // NEW: serialize leader request handling

    std::atomic<bool> running = false;

    

    std::map<int, nlohmann::json> preprepareCache;
    std::atomic<bool> fillHolePending{false};
    int fillHoleFromSeq{0};
    int fillHoleToSeq{0};
    std::chrono::steady_clock::time_point fillHoleDeadline;
    int fillHoleTimeoutMs{600}; // adjust as needed

    // Serialize message handling across TCP and gRPC
    std::mutex eventMtx;

    // gRPC server running inside this entity
    std::unique_ptr<NodeServiceImpl> grpcSvc_;
    std::unique_ptr<grpc::Server> grpcServer_;
    std::thread grpcThread_;
    void startGrpcServer();
    void stopGrpcServer();

    // gRPC clients for inter-node messaging (peerId -> stub to port 15000+peerId)
    std::mutex grpcStubsMtx_;
    std::unordered_map<int, std::unique_ptr<bedrock::Node::Stub>> grpcStubs_;
    void initGrpcStubs();             // create stubs for all peers and self
    bedrock::Node::Stub* getStub(int peerId);

    // Learning agent connection (optional, per-node)
    int agentPort{-1};
    std::unique_ptr<LearningAgent::Stub> agentStub_;

    // Agent episode / reward state
    uint32_t agentEpisode{1};

    std::mutex pendingTimeoutMtx;
    bool pendingTimeoutReady{false};
    SbftTimeout pendingTimeout;
    std::atomic<bool> agentStopPolling{false};

    SbftTimeout activeTimeoutSnapshot; // timeout active during current episode

    struct SavedEpisodeReward {
        uint32_t episode{0};
        SbftReport report;
        SbftTimeout timeoutUsed;
    };
    bool hasSavedReward{false};
    SavedEpisodeReward savedReward;

};

