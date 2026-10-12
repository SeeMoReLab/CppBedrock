#include "core/agent/AgentClient.h"

#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "core/Log.h"

namespace {

// Nearest rank: sorted[ceil(p*n)-1].
long long percentile(const std::vector<long long>& sorted, double p) {
    if (sorted.empty()) return 0;
    long long rank = (long long)std::ceil(p * (double)sorted.size()) - 1;
    return sorted[(size_t)std::max(0LL, rank)];
}

// Mean and p50/p90/p95 of raw samples, each divided by unit (1000 turns
// microseconds into ms; 1 for counts).
AgentStats windowStats(std::vector<long long> samples, double unit) {
    AgentStats s;
    if (samples.empty()) return s;
    std::sort(samples.begin(), samples.end());
    long long sum = std::accumulate(samples.begin(), samples.end(), 0LL);
    s.avg = (float)((double)sum / (double)samples.size() / unit);
    s.p50 = (float)((double)percentile(samples, 0.50) / unit);
    s.p90 = (float)((double)percentile(samples, 0.90) / unit);
    s.p95 = (float)((double)percentile(samples, 0.95) / unit);
    return s;
}

// Fills the base fields shared by PbftReport, SbftReport, and HotstuffReport
// (identical setter names in every generated class).
template <typename ReportT>
void fillBaseReport(ReportT& rep, const AgentMetricsSnapshot& m, const AgentTimeouts& t) {
    rep.set_total_transactions(m.totalTransactions);
    rep.set_total_consensus_instances(m.totalConsensus);
    rep.set_avg_consensus_latency_ms(m.latencyMs.avg);
    rep.set_p50_consensus_latency_ms(m.latencyMs.p50);
    rep.set_p90_consensus_latency_ms(m.latencyMs.p90);
    rep.set_p95_consensus_latency_ms(m.latencyMs.p95);
    rep.set_throughput_tps(m.throughputTps);
    rep.set_avg_batch_size(m.batchSize.avg);
    rep.set_p50_batch_size(m.batchSize.p50);
    rep.set_p90_batch_size(m.batchSize.p90);
    rep.set_p95_batch_size(m.batchSize.p95);
    rep.set_leader_change_count(m.leaderChangeCount);
    rep.set_regency_change_count(m.regencyChangeCount);
    rep.set_timeout_ms((uint32_t)std::max(0, t.electionMs));
    rep.set_avg_inter_commit_gap_ms(m.interCommitGapMs.avg);
    rep.set_p50_inter_commit_gap_ms(m.interCommitGapMs.p50);
    rep.set_p90_inter_commit_gap_ms(m.interCommitGapMs.p90);
    rep.set_p95_inter_commit_gap_ms(m.interCommitGapMs.p95);
    rep.set_view_change_count(m.viewChangeCount);
    rep.set_no_progress_view_change_count(m.noProgressViewChangeCount);
}

// Fills the phase families shared by PbftReport and SbftReport; the
// unprefixed phase fields are the window means.
template <typename ReportT>
void fillPhaseReport(ReportT& rep, const AgentMetricsSnapshot& m) {
    rep.set_pre_prepare_latency_ms(m.phase1Ms.avg);
    rep.set_p50_pre_prepare_latency_ms(m.phase1Ms.p50);
    rep.set_p90_pre_prepare_latency_ms(m.phase1Ms.p90);
    rep.set_p95_pre_prepare_latency_ms(m.phase1Ms.p95);
    rep.set_prepare_latency_ms(m.phase2Ms.avg);
    rep.set_p50_prepare_latency_ms(m.phase2Ms.p50);
    rep.set_p90_prepare_latency_ms(m.phase2Ms.p90);
    rep.set_p95_prepare_latency_ms(m.phase2Ms.p95);
    rep.set_commit_latency_ms(m.phase3Ms.avg);
    rep.set_p50_commit_latency_ms(m.phase3Ms.p50);
    rep.set_p90_commit_latency_ms(m.phase3Ms.p90);
    rep.set_p95_commit_latency_ms(m.phase3Ms.p95);
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
        fillBaseReport(rep, m, t);
        fillPhaseReport(rep, m);
    }
};

class SbftAgentAdapter : public ProtocolAgentAdapter {
public:
    Protocol protocol() const override { return PROTOCOL_SBFT; }
    std::string name() const override { return "SBFT"; }

    void fillState(ReportLocal& report,
                   const AgentMetricsSnapshot& m,
                   const AgentTimeouts& t) const override {
        buildReport(*report.mutable_sbft_state(), m, t);
    }

    void fillReward(Reward& reward,
                    uint32_t episode,
                    const AgentMetricsSnapshot& m,
                    const AgentTimeouts& t) const override {
        auto* r = reward.mutable_sbft();
        r->set_episode(episode);
        buildReport(*r->mutable_report(), m, t);
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
    static void buildReport(SbftReport& rep,
                            const AgentMetricsSnapshot& m,
                            const AgentTimeouts& t) {
        fillBaseReport(rep, m, t);
        fillPhaseReport(rep, m);
        rep.set_slow_path_timeout_ms((uint32_t)std::max(0, t.slowPathMs));
        rep.set_fast_path_count(m.fastPathCount);
        rep.set_slow_path_count(m.slowPathCount);
        rep.set_nth_prepare_gap_ms(m.nthPrepareGapMs);
        rep.set_nth_prepare_seen_count(m.nthPrepareSeenCount);
        rep.set_nth_prepare_unseen_count(m.nthPrepareUnseenCount);
    }
};

class HotStuffAgentAdapter : public ProtocolAgentAdapter {
public:
    Protocol protocol() const override { return PROTOCOL_HOTSTUFF; }
    std::string name() const override { return "HotStuff"; }

    void fillState(ReportLocal& report,
                   const AgentMetricsSnapshot& m,
                   const AgentTimeouts& t) const override {
        fillBaseReport(*report.mutable_hotstuff_state(), m, t);
    }

    void fillReward(Reward& reward,
                    uint32_t episode,
                    const AgentMetricsSnapshot& m,
                    const AgentTimeouts& t) const override {
        auto* r = reward.mutable_hotstuff();
        r->set_episode(episode);
        fillBaseReport(*r->mutable_report(), m, t);
        r->mutable_timeout_used()->set_timeout_delay_milliseconds(
            (uint32_t)std::max(0, t.electionMs));
    }

    bool extractTimeouts(const Timeout& timeout, AgentTimeouts& out) const override {
        if (!timeout.has_hotstuff()) return false;
        out.electionMs = (int)timeout.hotstuff().timeout_delay_milliseconds();
        out.slowPathMs = 0;
        return true;
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
    throw std::invalid_argument("no learning-agent report is defined in proto/agent.proto for protocol '" +
                                protocolName + "'");
}

// ---- MetricsWindow --------------------------------------------------------

void AgentClient::MetricsWindow::reset(std::chrono::steady_clock::time_point start) {
    latenciesUs.clear();
    batchSizes.clear();
    transactions = 0;
    phase1Us.clear();
    phase2Us.clear();
    phase3Us.clear();
    interCommitGapsUs.clear();
    hasLastCommit = false;
    viewChanges = 0;
    noProgressViewChanges = 0;
    newViews = 0;
    fastPath = 0;
    slowPath = 0;
    nthPrepareGapsUs.clear();
    nthPrepareUnseen = 0;
    windowStart = start;
}

AgentMetricsSnapshot AgentClient::MetricsWindow::snapshot(
    std::chrono::steady_clock::time_point end) const {
    AgentMetricsSnapshot s;
    s.totalTransactions = transactions;
    s.totalConsensus = static_cast<uint32_t>(batchSizes.size());
    s.batchSize = windowStats(batchSizes, 1.0);
    s.viewChangeCount = viewChanges;
    s.noProgressViewChangeCount = noProgressViewChanges;
    s.leaderChangeCount = newViews;
    s.regencyChangeCount = newViews;
    s.fastPathCount = fastPath;
    s.slowPathCount = slowPath;
    s.nthPrepareGapMs = windowStats(nthPrepareGapsUs, 1000.0).p50;
    s.nthPrepareSeenCount = static_cast<uint32_t>(nthPrepareGapsUs.size());
    s.nthPrepareUnseenCount = nthPrepareUnseen;
    s.phase1Ms = windowStats(phase1Us, 1000.0);
    s.phase2Ms = windowStats(phase2Us, 1000.0);
    s.phase3Ms = windowStats(phase3Us, 1000.0);
    s.latencyMs = windowStats(latenciesUs, 1000.0);
    s.interCommitGapMs = windowStats(interCommitGapsUs, 1000.0);
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - windowStart).count();
    if (elapsedMs > 0) {
        s.throughputTps = (float)((double)transactions * 1000.0 / (double)elapsedMs);
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
    if (config_.anchorUnixMs > 0 &&
        (long long)config_.featureDurationMs + config_.replyWaitMs + config_.warmupDurationMs +
                config_.rewardDurationMs <= 0) {
        throw std::invalid_argument("AgentClient episode grid requires a positive episode period");
    }
}

AgentClient::~AgentClient() { stop(); }

std::string AgentClient::logPrefix() const {
    return "[agent:" + adapter_->name() + "] ";
}

void AgentClient::start() {
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(config_.port),
                                       grpc::InsecureChannelCredentials());
    stub_ = LearningAgent::NewStub(channel);
    LOG_INFO(logPrefix() << "connected on port " << config_.port
             << " wall-clock windows: feature=" << config_.featureDurationMs
             << "ms reply_wait=" << config_.replyWaitMs
             << "ms warmup=" << config_.warmupDurationMs
             << "ms reward=" << config_.rewardDurationMs << "ms");

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(config_.rpcTimeoutMs));
    google::protobuf::Empty req, resp;
    auto st = stub_->Reset(&ctx, req, &resp);
    if (st.ok()) {
        LOG_INFO(logPrefix() << "agent reset OK");
    } else {
        LOG_WARN(logPrefix() << "agent reset failed (agent may not be running): " << st.error_message());
    }

    loopThread_ = std::thread(&AgentClient::run, this);
}

void AgentClient::stop() {
    {
        std::lock_guard<std::mutex> lk(stopMtx_);
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
        const auto now = std::chrono::steady_clock::now();
        lastSequence_ = sample.sequence;
        ++commitsSinceViewChange_;
        if (!sawFirstSample_) {
            sawFirstSample_ = true;
            firstSampleTime_ = now;
            notify = true;
        }
        if (stage_ == Stage::Feature || stage_ == Stage::Reward) {
            metrics_.transactions += sample.transactions;
            metrics_.batchSizes.push_back(sample.batchSize);
            if (sample.latencyUs >= 0) metrics_.latenciesUs.push_back(sample.latencyUs);
            if (sample.phase1Us >= 0) metrics_.phase1Us.push_back(sample.phase1Us);
            if (sample.phase2Us >= 0) metrics_.phase2Us.push_back(sample.phase2Us);
            if (sample.phase3Us >= 0) metrics_.phase3Us.push_back(sample.phase3Us);
            if (metrics_.hasLastCommit) {
                metrics_.interCommitGapsUs.push_back(
                    std::chrono::duration_cast<std::chrono::microseconds>(now - metrics_.lastCommitTime).count());
            }
            metrics_.hasLastCommit = true;
            metrics_.lastCommitTime = now;
            if (sample.path == 1) ++metrics_.fastPath;
            else if (sample.path == 0) ++metrics_.slowPath;
            if (sample.nthPrepareGapUs >= 0) metrics_.nthPrepareGapsUs.push_back(sample.nthPrepareGapUs);
            else if (sample.nthPrepareGapUs == -2) ++metrics_.nthPrepareUnseen;
        }
    }
    if (notify) stopCv_.notify_all();
}

void AgentClient::recordViewChange() {
    std::lock_guard<std::mutex> lk(mtx_);
    const bool noProgress = commitsSinceViewChange_ == 0;
    commitsSinceViewChange_ = 0;
    if (stage_ == Stage::Feature || stage_ == Stage::Reward) {
        ++metrics_.viewChanges;
        if (noProgress) ++metrics_.noProgressViewChanges;
    }
}

void AgentClient::recordNewView() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (stage_ == Stage::Feature || stage_ == Stage::Reward) {
        ++metrics_.newViews;
    }
}

bool AgentClient::waitUntil(std::chrono::steady_clock::time_point deadline) {
    std::unique_lock<std::mutex> lk(stopMtx_);
    return !stopCv_.wait_until(lk, deadline, [this] { return stopRequested_; });
}

void AgentClient::run() {
    using std::chrono::milliseconds;
    using std::chrono::steady_clock;
    using std::chrono::system_clock;

    // Shared episode grid: episode e (>= 1) applies at anchor + period*(e-1) and
    // starts applyOffset earlier. The anchor is converted to steady_clock once;
    // every deadline is then arithmetic on e.
    const bool anchored = config_.anchorUnixMs > 0;
    const long long periodMs = (long long)config_.featureDurationMs + config_.replyWaitMs +
                               config_.warmupDurationMs + config_.rewardDurationMs;
    const long long applyOffsetMs = (long long)config_.featureDurationMs + config_.replyWaitMs;
    steady_clock::time_point firstApply{};
    auto gridStart = [&](uint32_t e) {
        return firstApply + milliseconds(periodMs * (long long)(e - 1) - applyOffsetMs);
    };
    auto gridApplyUnixMs = [&](uint32_t e) {
        return (long long)config_.anchorUnixMs + periodMs * (long long)(e - 1);
    };
    // Smallest e >= 1 whose start is not before t.
    auto firstEpisodeFrom = [&](steady_clock::time_point t) -> uint32_t {
        const auto behind = t - gridStart(1);
        if (behind <= steady_clock::duration::zero()) return 1;
        const steady_clock::duration period = milliseconds(periodMs);
        return 1 + (uint32_t)((behind + period - steady_clock::duration(1)) / period);
    };
    // Idle until the grid start, then open the feature window.
    auto beginGridEpisode = [&](uint32_t e, steady_clock::time_point startAt) {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            episode_ = e;
            stage_ = Stage::Idle;
        }
        if (!waitUntil(startAt)) return false;
        std::lock_guard<std::mutex> lk(mtx_);
        stage_ = Stage::Feature;
        metrics_.reset(steady_clock::now());
        episodeStartTick_ = lastSequence_;
        return true;
    };

    steady_clock::time_point episodeStart;
    if (anchored) {
        const auto systemNow = system_clock::now();
        const auto steadyNow = steady_clock::now();
        const system_clock::time_point anchor{milliseconds(config_.anchorUnixMs)};
        firstApply = steadyNow + std::chrono::duration_cast<steady_clock::duration>(anchor - systemNow);
        const uint32_t first = firstEpisodeFrom(steadyNow);
        LOG_INFO(logPrefix() << "episode grid: anchor_unix_ms=" << config_.anchorUnixMs
                 << " period_ms=" << periodMs
                 << " apply_offset_ms=" << applyOffsetMs
                 << " first_episode=" << first
                 << " first_apply_unix_ms=" << gridApplyUnixMs(first));
        episodeStart = gridStart(first);
        if (!beginGridEpisode(first, episodeStart)) return;
    } else {
        // Idle until the first consensus sample arrives, mirroring the SmartBFT
        // wall-clock learning manager.
        LOG_INFO(logPrefix() << "episode anchor: first commit (no grid anchor)");
        {
            std::unique_lock<std::mutex> lk(stopMtx_);
            stopCv_.wait(lk, [this] {
                if (stopRequested_) return true;
                std::lock_guard<std::mutex> mlk(mtx_);
                return sawFirstSample_;
            });
            if (stopRequested_) return;
        }

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
        std::string gridLog;
        if (anchored) {
            const long long nowUnixMs = std::chrono::duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()).count();
            gridLog = " grid_apply_unix_ms=" + std::to_string(gridApplyUnixMs(episode)) +
                      " lag_ms=" + std::to_string(nowUnixMs - gridApplyUnixMs(episode));
        }

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
            LOG_INFO(logPrefix() << "applied recommendation: episode=" << episode
                     << " election " << previous.electionMs << "->" << next.electionMs << "ms"
                     << " slow_path " << previous.slowPathMs << "->" << next.slowPathMs << "ms"
                     << gridLog);
        } else {
            LOG_WARN(logPrefix() << "reply deadline reached without recommendation: episode=" << episode
                     << gridLog);
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
        // On the grid rewardEnd is the next episode's start; already past means
        // this episode overran its slot.
        const bool overran = anchored && steady_clock::now() > rewardEnd;
        if (!waitUntil(rewardEnd)) return;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            AgentMetricsSnapshot snap = metrics_.snapshot(std::chrono::steady_clock::now());
            // An overrun leaves a near-empty reward window; sending it would
            // score as a stall, so it is dropped.
            if (!overran) {
                pendingReward_.Clear();
                adapter_->fillReward(pendingReward_, episode_, snap, lastTimeouts_);
                hasPendingReward_ = true;
            }

            LOG_INFO(logPrefix() << "captured reward: episode=" << episode_
                     << " total_consensus=" << snap.totalConsensus
                     << " throughput_tps=" << snap.throughputTps
                     << " view_changes=" << snap.viewChangeCount);

            ++episode_;
            episodeStartTick_ = lastSequence_;
            metrics_.reset(std::chrono::steady_clock::now());
            stage_ = Stage::Feature;
        }
        if (!anchored) {
            episodeStart = rewardEnd;
            continue;
        }
        uint32_t next;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            next = episode_;
        }
        episodeStart = gridStart(next);
        if (overran) {
            const uint32_t skipTo = firstEpisodeFrom(steady_clock::now());
            LOG_WARN(logPrefix() << "episode overran its grid slot: episode=" << next
                     << " start already past; reward dropped; skipping to episode=" << skipTo);
            episodeStart = gridStart(skipTo);
            if (!beginGridEpisode(skipTo, episodeStart)) return;
        }
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
    if (st.ok()) {
        LOG_INFO(logPrefix() << "sent report: episode=" << episode
                 << " start_tick=" << report.start_tick()
                 << " report_seq=" << report.report_seq()
                 << (report.has_reward() ? " with_reward=1" : " with_reward=0"));
    } else {
        LOG_WARN(logPrefix() << "SendReport failed: episode=" << episode << " err=" << st.error_message());
    }
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
                LOG_INFO(logPrefix() << "timeout READY: episode=" << episode
                         << " election=" << out.electionMs << "ms"
                         << " slow_path=" << out.slowPathMs << "ms");
                return;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.pollIntervalMs));
    }
}
