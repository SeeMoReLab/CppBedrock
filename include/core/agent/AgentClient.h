#pragma once

// Protocol-agnostic learning-agent client for CppBedrock.
//
// Runs the wall-clock learning cycle against a per-node LearningAgent gRPC
// service (proto/agent.proto, the repo-wide contract shared with SmartBFT,
// Tendermint, and the Rust protocols):
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
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "proto/agent.grpc.pb.h"
#include "proto/agent.pb.h"

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
    uint32_t transactions{1};
    uint32_t batchSize{1};
    long long latencyUs{-1};
    long long phase1Us{-1};  // protocol phase slot 1 (e.g. pre-prepare)
    long long phase2Us{-1};  // protocol phase slot 2 (e.g. prepare)
    long long phase3Us{-1};  // protocol phase slot 3 (e.g. commit)
    // Fast/slow path attribution for SBFT: -1 unknown, 0 slow, 1 fast.
    int path{-1};
    // SBFT, measured only by the replica that led the sequence: n-th prepare
    // arrival minus the 2f+1-th; -2 when the n-th had not arrived by
    // execution; -1 when this replica did not lead it.
    long long nthPrepareGapUs{-1};
};

// Mean and nearest-rank percentiles (sorted[ceil(p*n)-1]) of one window's
// samples; all zero without samples.
struct AgentStats {
    float avg{0.0f};
    float p50{0.0f};
    float p90{0.0f};
    float p95{0.0f};
};

// Aggregated metrics for one learning window. The base fields mirror the
// PbftReport layout shared by every protocol report in agent.proto.
// Latencies and gaps are ms; batch size counts requests per batch.
struct AgentMetricsSnapshot {
    uint32_t totalTransactions{0};
    uint32_t totalConsensus{0};
    AgentStats latencyMs;
    float throughputTps{0.0f};
    AgentStats batchSize;
    uint32_t leaderChangeCount{0};
    uint32_t regencyChangeCount{0};
    AgentStats interCommitGapMs;
    uint32_t viewChangeCount{0};
    uint32_t noProgressViewChangeCount{0};
    AgentStats phase1Ms;
    AgentStats phase2Ms;
    AgentStats phase3Ms;
    uint32_t fastPathCount{0};
    uint32_t slowPathCount{0};
    // SBFT, over the sequences this replica led in the window: median n-th
    // prepare gap of those whose n-th prepare arrived by execution, and the
    // counts with and without it.
    float nthPrepareGapMs{0.0f};
    uint32_t nthPrepareSeenCount{0};
    uint32_t nthPrepareUnseenCount{0};
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

    // Fill the protocol-specific reward oneof of a Reward. timeoutsUsed must
    // be echoed verbatim: the agent requires byte-identical timeout_used
    // across nodes to accept a reward.
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
// "ChainedHotstuff"). Throws std::invalid_argument for names without a report
// message in agent.proto (e.g. "Zyzzyva").
std::unique_ptr<ProtocolAgentAdapter> makeProtocolAgentAdapter(const std::string& protocolName);

struct AgentClientConfig {
    int nodeId{0};
    int port{-1};
    int featureDurationMs{10000};
    int replyWaitMs{2000};
    int warmupDurationMs{3000};
    int rewardDurationMs{5000};
    int pollIntervalMs{50};
    int rpcTimeoutMs{2000};
    // Shared episode grid (harness start_unix_ms + failure-spec warm-up, epoch
    // ms): episode e applies its timeouts at anchorUnixMs + period*(e-1), where
    // period is the sum of the four durations, and starts feature + reply wait
    // earlier. 0 anchors episode 1 on the first commit instead; that path is
    // for library tests only, the replica CLI requires an anchor.
    std::uint64_t anchorUnixMs{0};
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
    // cycle stays idle until the first grid episode start at or after now (or,
    // without an anchor, until the first consensus sample arrives).
    void start();
    void stop();

    // Hooks called by the protocol engine.
    void recordConsensus(const AgentConsensusSample& sample);
    // A local view-change timer fired (an election was started).
    void recordViewChange();
    // A new view was installed (leadership moved).
    void recordNewView();

private:
    enum class Stage { Idle, Feature, ReplyWait, Warmup, Reward };

    struct MetricsWindow {
        std::vector<long long> latenciesUs;
        std::vector<long long> batchSizes;
        uint32_t transactions{0};
        std::vector<long long> phase1Us;
        std::vector<long long> phase2Us;
        std::vector<long long> phase3Us;
        // Gaps between two commits inside this window: n commits give n-1.
        std::vector<long long> interCommitGapsUs;
        bool hasLastCommit{false};
        std::chrono::steady_clock::time_point lastCommitTime{};
        uint32_t newViews{0};
        uint32_t fastPath{0};
        uint32_t slowPath{0};
        std::vector<long long> nthPrepareGapsUs;
        uint32_t nthPrepareUnseen{0};
        std::chrono::steady_clock::time_point windowStart{};

        void reset(std::chrono::steady_clock::time_point start);
        AgentMetricsSnapshot snapshot(std::chrono::steady_clock::time_point end) const;
    };

    void run();
    // Sleeps until the deadline; returns false when stop() was requested.
    bool waitUntil(std::chrono::steady_clock::time_point deadline);
    void sendReport(uint32_t episode);
    // Sets the snapshot's view-change counts to those of the last episode period
    // before `end` (caller holds mtx_).
    void countRecentViewChanges(AgentMetricsSnapshot& snap,
                                std::chrono::steady_clock::time_point end);
    void pollForTimeout(uint32_t episode);
    void startPolling(uint32_t episode);
    void stopPolling();
    std::string logPrefix() const;

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
    uint64_t commitsSinceViewChange_{0};
    // Every view change with whether it came with no commit since the previous one,
    // whatever the stage, for the counts over the last episode (2026-10-19: a hold
    // re-pin lands on the episode grid, in the warm-up between the windows, and a
    // window-only count missed every one of those depositions).
    std::deque<std::pair<std::chrono::steady_clock::time_point, bool>> viewChangeTimes_;
    AgentTimeouts currentTimeouts_{};
    AgentTimeouts lastTimeouts_{};  // timeouts active during the reward window
    bool decisionReady_{false};
    AgentTimeouts decision_{};
    bool hasPendingReward_{false};
    Reward pendingReward_;
    std::chrono::steady_clock::time_point firstSampleTime_{};
    bool sawFirstSample_{false};
};
