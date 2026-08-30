#include "../../../include/core/agent/AgentClient.h"

#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

long long percentile(const std::vector<long long>& sorted, double p) {
    if (sorted.empty()) return 0;
    double idx = p * (double)(sorted.size() - 1);
    size_t lo = (size_t)idx;
    size_t hi = std::min(sorted.size() - 1, lo + 1);
    double f = idx - (double)lo;
    return (long long)((1.0 - f) * (double)sorted[lo] + f * (double)sorted[hi]);
}

float avgMs(const std::vector<long long>& us) {
    if (us.empty()) return 0.0f;
    long long sum = std::accumulate(us.begin(), us.end(), 0LL);
    return (float)((double)sum / (double)us.size() / 1000.0);
}

// ---- Adapters -------------------------------------------------------------

class PbftAgentAdapter : public ProtocolAgentAdapter {
public:
    Protocol protocol() const override { return PROTOCOL_PBFT; }
    std::string name() const override { return "PBFT"; }

    void fillState(ReportLocal& report,
                   const AgentMetricsSnapshot& m,
                   const AgentTimeouts& t) const override {
        buildReport(*report.mutable_pbft_state(), m, t);
    }

    void fillReward(Reward& reward,
                    uint32_t episode,
                    const AgentMetricsSnapshot& m,
                    const AgentTimeouts& t) const override {
        auto* r = reward.mutable_pbft();
        r->set_episode(episode);
        buildReport(*r->mutable_report(), m, t);
        r->mutable_timeout_used()->set_election_timeout_milliseconds(
            (uint32_t)std::max(0, t.electionMs));
    }

    bool extractTimeouts(const Timeout& timeout, AgentTimeouts& out) const override {
        if (!timeout.has_pbft()) return false;
        out.electionMs = (int)timeout.pbft().election_timeout_milliseconds();
        out.slowPathMs = 0;
        return true;
    }

private:
    static void buildReport(PbftReport& rep,
                            const AgentMetricsSnapshot& m,
                            const AgentTimeouts& t) {
        rep.set_total_transactions(m.totalTransactions);
        rep.set_total_consensus_instances(m.totalConsensus);
        rep.set_avg_consensus_latency_ms(m.avgLatencyMs);
        rep.set_p50_consensus_latency_ms(m.p50LatencyMs);
        rep.set_p95_consensus_latency_ms(m.p95LatencyMs);
        rep.set_p99_consensus_latency_ms(m.p99LatencyMs);
        rep.set_throughput_tps(m.throughputTps);
        rep.set_view_change_count(m.viewChangeCount);
        rep.set_timeout_ms((uint32_t)std::max(0, t.electionMs));
        rep.set_pre_prepare_latency_ms(m.phase1AvgMs);
        rep.set_prepare_latency_ms(m.phase2AvgMs);
        rep.set_commit_latency_ms(m.phase3AvgMs);
    }
};

class SbftAgentAdapter : public ProtocolAgentAdapter {
public:
    Protocol protocol() const override { return PROTOCOL_SBFT; }
    std::string name() const override { return "SBFT"; }

    void fillState(ReportLocal& report,
                   const AgentMetricsSnapshot& m,
                   const AgentTimeouts& t) const override {
        (void)t;
        buildReport(*report.mutable_sbft_state(), m);
    }

    void fillReward(Reward& reward,
                    uint32_t episode,
                    const AgentMetricsSnapshot& m,
                    const AgentTimeouts& t) const override {
        auto* r = reward.mutable_sbft();
        r->set_episode(episode);
        buildReport(*r->mutable_report(), m);
        auto* used = r->mutable_timeout_used();
        used->set_election_timeout_milliseconds((uint32_t)std::max(0, t.electionMs));
        used->set_slow_path_timeout_milliseconds((uint32_t)std::max(0, t.slowPathMs));
    }

    bool extractTimeouts(const Timeout& timeout, AgentTimeouts& out) const override {
        if (!timeout.has_sbft()) return false;
        out.electionMs = (int)timeout.sbft().election_timeout_milliseconds();
        out.slowPathMs = (int)timeout.sbft().slow_path_timeout_milliseconds();
        return true;
    }

private:
    static void buildReport(SbftReport& rep, const AgentMetricsSnapshot& m) {
        rep.set_total_transactions(m.totalTransactions);
        rep.set_total_consensus_instances(m.totalConsensus);
        rep.set_avg_consensus_latency_ms(m.avgLatencyMs);
        rep.set_p95_consensus_latency_ms(m.p95LatencyMs);
        rep.set_p99_consensus_latency_ms(m.p99LatencyMs);
        rep.set_throughput_tps(m.throughputTps);
        rep.set_leader_change_count(m.viewChangeCount);
        rep.set_pre_prepare_latency_ms(m.phase1AvgMs);
        rep.set_prepare_latency_ms(m.phase2AvgMs);
        rep.set_commit_latency_ms(m.phase3AvgMs);
    }
};

class HotStuffAgentAdapter : public ProtocolAgentAdapter {
public:
    Protocol protocol() const override { return PROTOCOL_HOTSTUFF; }
    std::string name() const override { return "HotStuff"; }

    void fillState(ReportLocal& report,
                   const AgentMetricsSnapshot& m,
                   const AgentTimeouts& t) const override {
        buildReport(*report.mutable_hotstuff_state(), m, t);
    }

    void fillReward(Reward& reward,
                    uint32_t episode,
                    const AgentMetricsSnapshot& m,
                    const AgentTimeouts& t) const override {
        auto* r = reward.mutable_hotstuff();
        r->set_episode(episode);
        buildReport(*r->mutable_report(), m, t);
        r->mutable_timeout_used()->set_new_view_timeout_milliseconds(
            (uint32_t)std::max(0, t.electionMs));
    }

    bool extractTimeouts(const Timeout& timeout, AgentTimeouts& out) const override {
        if (!timeout.has_hotstuff()) return false;
        out.electionMs = (int)timeout.hotstuff().new_view_timeout_milliseconds();
        out.slowPathMs = 0;
        return true;
    }

private:
    static void buildReport(HotStuffReport& rep,
                            const AgentMetricsSnapshot& m,
                            const AgentTimeouts& t) {
        rep.set_total_transactions(m.totalTransactions);
        rep.set_total_consensus_instances(m.totalConsensus);
        rep.set_avg_consensus_latency_ms(m.avgLatencyMs);
        rep.set_p50_consensus_latency_ms(m.p50LatencyMs);
        rep.set_p95_consensus_latency_ms(m.p95LatencyMs);
        rep.set_p99_consensus_latency_ms(m.p99LatencyMs);
        rep.set_throughput_tps(m.throughputTps);
        rep.set_new_view_count(m.viewChangeCount);
        rep.set_timeout_ms((uint32_t)std::max(0, t.electionMs));
        rep.set_prepare_latency_ms(m.phase1AvgMs);
        rep.set_pre_commit_latency_ms(m.phase2AvgMs);
        rep.set_commit_latency_ms(m.phase3AvgMs);
    }
};

class ZyzzyvaAgentAdapter : public ProtocolAgentAdapter {
public:
    Protocol protocol() const override { return PROTOCOL_ZYZZYVA; }
    std::string name() const override { return "Zyzzyva"; }

    void fillState(ReportLocal& report,
                   const AgentMetricsSnapshot& m,
                   const AgentTimeouts& t) const override {
        buildReport(*report.mutable_zyzzyva_state(), m, t);
    }

    void fillReward(Reward& reward,
                    uint32_t episode,
                    const AgentMetricsSnapshot& m,
                    const AgentTimeouts& t) const override {
        auto* r = reward.mutable_zyzzyva();
        r->set_episode(episode);
        buildReport(*r->mutable_report(), m, t);
        r->mutable_timeout_used()->set_election_timeout_milliseconds(
            (uint32_t)std::max(0, t.electionMs));
    }

    bool extractTimeouts(const Timeout& timeout, AgentTimeouts& out) const override {
        if (!timeout.has_zyzzyva()) return false;
        out.electionMs = (int)timeout.zyzzyva().election_timeout_milliseconds();
        out.slowPathMs = 0;
        return true;
    }

private:
    static void buildReport(ZyzzyvaReport& rep,
                            const AgentMetricsSnapshot& m,
                            const AgentTimeouts& t) {
        rep.set_total_transactions(m.totalTransactions);
        rep.set_total_consensus_instances(m.totalConsensus);
        rep.set_avg_consensus_latency_ms(m.avgLatencyMs);
        rep.set_p50_consensus_latency_ms(m.p50LatencyMs);
        rep.set_p95_consensus_latency_ms(m.p95LatencyMs);
        rep.set_p99_consensus_latency_ms(m.p99LatencyMs);
        rep.set_throughput_tps(m.throughputTps);
        rep.set_view_change_count(m.viewChangeCount);
        rep.set_timeout_ms((uint32_t)std::max(0, t.electionMs));
        rep.set_speculative_execute_latency_ms(m.phase1AvgMs);
    }
};

}  // namespace

std::unique_ptr<ProtocolAgentAdapter> makeProtocolAgentAdapter(const std::string& protocolName) {
    if (protocolName == "PBFT" || protocolName == "LinearPBFT") {
        return std::make_unique<PbftAgentAdapter>();
    }
    if (protocolName == "SBFT") {
        return std::make_unique<SbftAgentAdapter>();
    }
    if (protocolName == "Hotstuff" || protocolName == "Hotstuff2" ||
        protocolName == "ChainedHotstuff") {
        return std::make_unique<HotStuffAgentAdapter>();
    }
    if (protocolName == "Zyzzyva") {
        return std::make_unique<ZyzzyvaAgentAdapter>();
    }
    throw std::invalid_argument("no learning-agent adapter for protocol '" + protocolName + "'");
}

// ---- MetricsWindow --------------------------------------------------------

void AgentClient::MetricsWindow::reset(std::chrono::steady_clock::time_point start) {
    latenciesUs.clear();
    phase1Us.clear();
    phase2Us.clear();
    phase3Us.clear();
    viewChanges = 0;
    windowStart = start;
}

AgentMetricsSnapshot AgentClient::MetricsWindow::snapshot(
    std::chrono::steady_clock::time_point end) const {
    AgentMetricsSnapshot s;
    s.totalTransactions = (uint32_t)latenciesUs.size();
    s.totalConsensus = (uint32_t)latenciesUs.size();
    s.viewChangeCount = viewChanges;
    s.phase1AvgMs = avgMs(phase1Us);
    s.phase2AvgMs = avgMs(phase2Us);
    s.phase3AvgMs = avgMs(phase3Us);
    if (!latenciesUs.empty()) {
        std::vector<long long> sorted = latenciesUs;
        std::sort(sorted.begin(), sorted.end());
        s.avgLatencyMs = avgMs(latenciesUs);
        s.p50LatencyMs = (float)((double)percentile(sorted, 0.50) / 1000.0);
        s.p95LatencyMs = (float)((double)percentile(sorted, 0.95) / 1000.0);
        s.p99LatencyMs = (float)((double)percentile(sorted, 0.99) / 1000.0);
    }
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - windowStart).count();
    if (elapsedMs > 0) {
        s.throughputTps = (float)((double)latenciesUs.size() * 1000.0 / (double)elapsedMs);
    }
    return s;
}

// ---- AgentClient ----------------------------------------------------------

AgentClient::AgentClient(AgentClientConfig config,
                         std::unique_ptr<ProtocolAgentAdapter> adapter,
                         AgentTimeouts initialTimeouts,
                         std::function<void(const AgentTimeouts&)> applyTimeouts)
    : config_(std::move(config)),
      adapter_(std::move(adapter)),
      applyTimeouts_(std::move(applyTimeouts)),
      currentTimeouts_(initialTimeouts),
      lastTimeouts_(initialTimeouts) {
    if (!adapter_) throw std::invalid_argument("AgentClient requires an adapter");
    if (config_.port <= 0) throw std::invalid_argument("AgentClient requires a positive agent port");
}

AgentClient::~AgentClient() { stop(); }

void AgentClient::logPrefix(std::ostream& os) const {
    os << "[Node " << config_.nodeId << "][agent:" << adapter_->name() << "] ";
}

void AgentClient::start() {
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(config_.port),
                                       grpc::InsecureChannelCredentials());
    stub_ = LearningAgent::NewStub(channel);
    {
        std::ostringstream os;
        logPrefix(os);
        os << "connected on port " << config_.port
           << " wall-clock windows: feature=" << config_.featureDurationMs
           << "ms reply_wait=" << config_.replyWaitMs
           << "ms warmup=" << config_.warmupDurationMs
           << "ms reward=" << config_.rewardDurationMs << "ms\n";
        std::cout << os.str();
    }

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(config_.rpcTimeoutMs));
    google::protobuf::Empty req, resp;
    auto st = stub_->Reset(&ctx, req, &resp);
    std::ostringstream os;
    logPrefix(os);
    if (st.ok()) {
        os << "agent reset OK\n";
    } else {
        os << "agent reset failed (agent may not be running): " << st.error_message() << "\n";
    }
    std::cout << os.str();

    loopThread_ = std::thread(&AgentClient::run, this);
}

void AgentClient::stop() {
    {
        std::lock_guard<std::mutex> lk(stopMtx_);
        if (stopRequested_) {
            // stop() may be called twice (explicitly and from the destructor).
        }
        stopRequested_ = true;
    }
    stopCv_.notify_all();
    stopPolling();
    if (loopThread_.joinable()) loopThread_.join();
}

void AgentClient::recordConsensus(const AgentConsensusSample& sample) {
    bool notify = false;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        lastSequence_ = sample.sequence;
        if (!sawFirstSample_) {
            sawFirstSample_ = true;
            firstSampleTime_ = std::chrono::steady_clock::now();
            notify = true;
        }
        if (stage_ == Stage::Feature || stage_ == Stage::Reward) {
            if (sample.latencyUs >= 0) metrics_.latenciesUs.push_back(sample.latencyUs);
            if (sample.phase1Us >= 0) metrics_.phase1Us.push_back(sample.phase1Us);
            if (sample.phase2Us >= 0) metrics_.phase2Us.push_back(sample.phase2Us);
            if (sample.phase3Us >= 0) metrics_.phase3Us.push_back(sample.phase3Us);
        }
    }
    if (notify) stopCv_.notify_all();
}

void AgentClient::recordViewChange() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (stage_ == Stage::Feature || stage_ == Stage::Reward) {
        ++metrics_.viewChanges;
    }
}

bool AgentClient::waitUntil(std::chrono::steady_clock::time_point deadline) {
    std::unique_lock<std::mutex> lk(stopMtx_);
    return !stopCv_.wait_until(lk, deadline, [this] { return stopRequested_; });
}

void AgentClient::run() {
    // Idle until the first consensus sample arrives, mirroring the SmartBFT
    // wall-clock learning manager.
    {
        std::unique_lock<std::mutex> lk(stopMtx_);
        stopCv_.wait(lk, [this] {
            if (stopRequested_) return true;
            std::lock_guard<std::mutex> mlk(mtx_);
            return sawFirstSample_;
        });
        if (stopRequested_) return;
    }

    std::chrono::steady_clock::time_point episodeStart;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        episodeStart = firstSampleTime_;
        stage_ = Stage::Feature;
        metrics_.reset(episodeStart);
        episodeStartTick_ = lastSequence_ > 0 ? lastSequence_ - 1 : 0;
    }

    while (true) {
        // --- Feature stage ---
        auto featureEnd = episodeStart + std::chrono::milliseconds(config_.featureDurationMs);
        if (!waitUntil(featureEnd)) return;

        uint32_t episode;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            episode = episode_;
            stage_ = Stage::ReplyWait;
            decisionReady_ = false;
        }
        sendReport(episode);
        startPolling(episode);

        // --- Reply wait ---
        auto applyAt = featureEnd + std::chrono::milliseconds(config_.replyWaitMs);
        if (!waitUntil(applyAt)) return;
        stopPolling();

        AgentTimeouts toApply{};
        bool haveDecision = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (decisionReady_) {
                toApply = decision_;
                haveDecision = true;
            }
            stage_ = Stage::Warmup;
        }
        if (haveDecision) {
            AgentTimeouts previous;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                previous = currentTimeouts_;
            }
            AgentTimeouts next = previous;
            if (toApply.electionMs > 0) next.electionMs = toApply.electionMs;
            if (toApply.slowPathMs > 0) next.slowPathMs = toApply.slowPathMs;
            applyTimeouts_(next);
            {
                std::lock_guard<std::mutex> lk(mtx_);
                currentTimeouts_ = next;
            }
            std::ostringstream os;
            logPrefix(os);
            os << "applied recommendation: episode=" << episode
               << " election " << previous.electionMs << "->" << next.electionMs << "ms"
               << " slow_path " << previous.slowPathMs << "->" << next.slowPathMs << "ms\n";
            std::cout << os.str();
        } else {
            std::ostringstream os;
            logPrefix(os);
            os << "reply deadline reached without recommendation: episode=" << episode << "\n";
            std::cout << os.str();
        }
        {
            std::lock_guard<std::mutex> lk(mtx_);
            lastTimeouts_ = currentTimeouts_;
        }

        // --- Warm-up (metrics discarded) ---
        auto rewardStart = applyAt + std::chrono::milliseconds(config_.warmupDurationMs);
        if (!waitUntil(rewardStart)) return;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            metrics_.reset(std::chrono::steady_clock::now());
            stage_ = Stage::Reward;
        }

        // --- Reward stage ---
        auto rewardEnd = rewardStart + std::chrono::milliseconds(config_.rewardDurationMs);
        if (!waitUntil(rewardEnd)) return;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            AgentMetricsSnapshot snap = metrics_.snapshot(std::chrono::steady_clock::now());
            pendingReward_.Clear();
            adapter_->fillReward(pendingReward_, episode_, snap, lastTimeouts_);
            hasPendingReward_ = true;

            std::ostringstream os;
            logPrefix(os);
            os << "captured reward: episode=" << episode_
               << " total_consensus=" << snap.totalConsensus
               << " throughput_tps=" << snap.throughputTps << "\n";
            std::cout << os.str();

            ++episode_;
            episodeStartTick_ = lastSequence_;
            metrics_.reset(std::chrono::steady_clock::now());
            stage_ = Stage::Feature;
        }
        episodeStart = rewardEnd;
    }
}

void AgentClient::sendReport(uint32_t episode) {
    ReportLocal report;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        AgentMetricsSnapshot snap = metrics_.snapshot(std::chrono::steady_clock::now());
        report.set_node_id((uint32_t)config_.nodeId);
        report.set_episode(episode);
        report.set_protocol(adapter_->protocol());
        report.set_start_tick(episodeStartTick_);
        report.set_report_seq(lastSequence_);
        report.set_window_consensus_count(0);  // wall-clock-bounded window
        adapter_->fillState(report, snap, currentTimeouts_);
        if (hasPendingReward_) {
            *report.mutable_reward() = pendingReward_;
            hasPendingReward_ = false;
        }
    }

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(config_.rpcTimeoutMs));
    google::protobuf::Empty resp;
    auto st = stub_->SendReport(&ctx, report, &resp);
    std::ostringstream os;
    logPrefix(os);
    if (st.ok()) {
        os << "sent report: episode=" << episode
           << " start_tick=" << report.start_tick()
           << " report_seq=" << report.report_seq()
           << (report.has_reward() ? " with_reward=1" : " with_reward=0") << "\n";
    } else {
        os << "SendReport failed: episode=" << episode << " err=" << st.error_message() << "\n";
    }
    std::cout << os.str();
}

void AgentClient::startPolling(uint32_t episode) {
    stopPolling();
    pollStop_ = false;
    pollThread_ = std::thread(&AgentClient::pollForTimeout, this, episode);
}

void AgentClient::stopPolling() {
    pollStop_ = true;
    if (pollThread_.joinable()) pollThread_.join();
}

void AgentClient::pollForTimeout(uint32_t episode) {
    while (!pollStop_.load()) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(config_.rpcTimeoutMs));
        TimeoutRequest req;
        req.set_episode(episode);
        req.set_protocol(adapter_->protocol());
        TimeoutStatus status;
        auto st = stub_->GetTimeout(&ctx, req, &status);
        if (st.ok() && status.status() == TimeoutStatus::READY && status.has_timeout() &&
            status.window_consensus_count() == 0) {
            AgentTimeouts out{};
            if (adapter_->extractTimeouts(status.timeout(), out)) {
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    decision_ = out;
                    decisionReady_ = true;
                }
                std::ostringstream os;
                logPrefix(os);
                os << "timeout READY: episode=" << episode
                   << " election=" << out.electionMs << "ms"
                   << " slow_path=" << out.slowPathMs << "ms\n";
                std::cout << os.str();
                return;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.pollIntervalMs));
    }
}
