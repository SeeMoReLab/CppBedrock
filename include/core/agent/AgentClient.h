#pragma once

// Protocol-agnostic learning-agent client for CppBedrock.
//
// Runs the wall-clock learning cycle against a per-node LearningAgent gRPC
// service (proto/agent.proto):
//   1. Feature stage  - collect consensus metrics for feature_duration_ms.
//   2. Reply wait     - send the report, poll for a timeout recommendation.
//   3. Warm-up        - apply the recommendation, discard metrics while the
//                       system settles for warmup_duration_ms.
//   4. Reward stage   - collect metrics for reward_duration_ms under the new
//                       timeouts; the result is attached as the reward to the
//                       next episode's report.
//
// Everything protocol-specific (which report message to fill, which timeout
// fields to read) lives in a ProtocolAgentAdapter, so any CppBedrock protocol
// can plug in by providing an adapter.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "proto/agent.grpc.pb.h"
#include "proto/agent.pb.h"
#include <iostream>

// Timeouts a recommendation can adjust. electionMs drives the view-change
// timer of every protocol; slowPathMs is only meaningful for SBFT.
struct AgentTimeouts {
    int electionMs{0};
    int slowPathMs{0};
};

// One completed consensus instance, as observed by the local node.
// Latencies are microseconds; use -1 when a value is unavailable.
struct AgentConsensusSample {
    uint32_t sequence{0};
    long long latencyUs{-1};
    long long phase1Us{-1};  // protocol phase slot 1 (e.g. pre-prepare)
    long long phase2Us{-1};  // protocol phase slot 2 (e.g. prepare)
    long long phase3Us{-1};  // protocol phase slot 3 (e.g. commit)
};

// Aggregated metrics for one learning window.
struct AgentMetricsSnapshot {
    uint32_t totalTransactions{0};
    uint32_t totalConsensus{0};
    float avgLatencyMs{0.0f};
    float p50LatencyMs{0.0f};
    float p95LatencyMs{0.0f};
    float p99LatencyMs{0.0f};
    float throughputTps{0.0f};
    float phase1AvgMs{0.0f};
    float phase2AvgMs{0.0f};
    float phase3AvgMs{0.0f};
    uint32_t viewChangeCount{0};
};

// Protocol-specific mapping between the generic metrics/timeouts and the
// per-protocol proto messages.
class ProtocolAgentAdapter {
public:
    virtual ~ProtocolAgentAdapter() = default;

    virtual Protocol protocol() const = 0;
    virtual std::string name() const = 0;

    // Fill the protocol-specific state oneof of a ReportLocal.
    virtual void fillState(ReportLocal& report,
                           const AgentMetricsSnapshot& metrics,
                           const AgentTimeouts& timeouts) const = 0;

    // Fill the protocol-specific reward oneof of a Reward.
    virtual void fillReward(Reward& reward,
                            uint32_t episode,
                            const AgentMetricsSnapshot& metrics,
                            const AgentTimeouts& timeoutsUsed) const = 0;

    // Extract the recommended timeouts. Returns false when the Timeout does
    // not carry this protocol's oneof value.
    virtual bool extractTimeouts(const Timeout& timeout,
                                 AgentTimeouts& out) const = 0;
};

// Returns the adapter for a CppBedrock protocol name as it appears in the
// protocol config YAML ("PBFT", "LinearPBFT", "SBFT", "Hotstuff", "Hotstuff2",
// "ChainedHotstuff", "Zyzzyva"). Throws std::invalid_argument for unknown
// names.
std::unique_ptr<ProtocolAgentAdapter> makeProtocolAgentAdapter(const std::string& protocolName);

struct AgentClientConfig {
    int nodeId{0};
    int port{-1};
    int featureDurationMs{8000};
    int replyWaitMs{2000};
    int warmupDurationMs{2000};
    int rewardDurationMs{8000};
    int pollIntervalMs{50};
    int rpcTimeoutMs{500};
};

class AgentClient {
public:
    // applyTimeouts is invoked from the learning thread when a recommendation
    // is applied; it must be safe to call concurrently with consensus
    // processing.
    AgentClient(AgentClientConfig config,
                std::unique_ptr<ProtocolAgentAdapter> adapter,
                AgentTimeouts initialTimeouts,
                std::function<void(const AgentTimeouts&)> applyTimeouts);
    ~AgentClient();

    AgentClient(const AgentClient&) = delete;
    AgentClient& operator=(const AgentClient&) = delete;

    // Connects to the agent, resets it, and starts the wall-clock cycle. The
    // cycle stays idle until the first consensus sample arrives.
    void start();
    void stop();

    // Hooks called by the protocol engine.
    void recordConsensus(const AgentConsensusSample& sample);
    void recordViewChange();

private:
    enum class Stage { Idle, Feature, ReplyWait, Warmup, Reward };

    struct MetricsWindow {
        std::vector<long long> latenciesUs;
        std::vector<long long> phase1Us;
        std::vector<long long> phase2Us;
        std::vector<long long> phase3Us;
        uint32_t viewChanges{0};
        std::chrono::steady_clock::time_point windowStart{};

        void reset(std::chrono::steady_clock::time_point start);
        AgentMetricsSnapshot snapshot(std::chrono::steady_clock::time_point end) const;
    };

    void run();
    // Sleeps until the deadline; returns false when stop() was requested.
    bool waitUntil(std::chrono::steady_clock::time_point deadline);
    void sendReport(uint32_t episode);
    void pollForTimeout(uint32_t episode);
    void startPolling(uint32_t episode);
    void stopPolling();
    void logPrefix(std::ostream& os) const;

    AgentClientConfig config_;
    std::unique_ptr<ProtocolAgentAdapter> adapter_;
    std::function<void(const AgentTimeouts&)> applyTimeouts_;

    std::unique_ptr<LearningAgent::Stub> stub_;

    // Learning loop thread and interruptible sleep.
    std::thread loopThread_;
    std::mutex stopMtx_;
    std::condition_variable stopCv_;
    bool stopRequested_{false};

    // Poll thread for GetTimeout.
    std::thread pollThread_;
    std::atomic<bool> pollStop_{false};

    // Shared state.
    mutable std::mutex mtx_;
    Stage stage_{Stage::Idle};
    MetricsWindow metrics_;
    uint32_t episode_{1};
    uint32_t episodeStartTick_{0};
    uint32_t lastSequence_{0};
    AgentTimeouts currentTimeouts_{};
    AgentTimeouts lastTimeouts_{};  // timeouts active during the reward window
    bool decisionReady_{false};
    AgentTimeouts decision_{};
    bool hasPendingReward_{false};
    Reward pendingReward_;
    std::chrono::steady_clock::time_point firstSampleTime_{};
    bool sawFirstSample_{false};
};
