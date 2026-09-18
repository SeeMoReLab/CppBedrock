// Benchmark client for CppBedrock replicas.
//
// Opens --connections client streams to every replica in the committee,
// submits requests at a fixed total rate (leader mode: everything to the
// current leader, following leader hints in replies; broadcast mode: every
// request to every replica), counts a request as committed once f+1
// distinct replicas replied, and prints one Monitor line per interval in
// the exact format the SmartBFT and Rust clients use:
//
//   [client <ts>] Monitor duration=1.000s trxs=N succ=N err=N tps=X avg_ms=X p50=N p95=N p99=N max=N

#include "core/Committee.h"
#include "core/Log.h"
#include "core/crypto/OpenSSLCryptoProvider.h"
#include "proto/bedrock.grpc.pb.h"
#include "proto/bedrock.pb.h"

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using Clock = std::chrono::steady_clock;

namespace {

constexpr int kTicksPerSecond = 20;
constexpr uint64_t kLaneShift = 48;

struct Options {
    std::string committeePath;
    double rate{1000.0};
    int durationS{0};
    int requestTimeoutMs{10000};
    std::string targetMode{"leader"};
    int connections{4};
    long long startUnixMs{0};
    int monitorIntervalMs{1000};
    std::string clientIdPrefix{"client"};
    bool sign{false};
    std::string clientKeyPath;
    int initialLeader{0};
    int connectTimeoutS{15};
};

std::atomic<bool> g_stop{false};
void onSignal(int) { g_stop.store(true); }

long long unixMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// ---- Metrics (mirrors SmartBFT/examples/smallbank/metrics.go) ----

struct Snapshot {
    int64_t success{0};
    int64_t errors{0};
    int64_t totalLatencyNs{0};
    int64_t maxLatencyMs{0};
    std::map<int64_t, int64_t> histogram;
};

class Metrics {
public:
    explicit Metrics(int requestTimeoutMs)
        : maxLatencyMs_(requestTimeoutMs > 0 ? std::max<int64_t>(1, requestTimeoutMs) : INT64_MAX) {}

    void record(bool success, std::chrono::nanoseconds latency) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (success) {
            ++success_;
            totalLatencyNs_ += latency.count();
            const int64_t bucket = std::min<int64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(latency).count(), maxLatencyMs_);
            ++histogram_[bucket];
        } else {
            ++errors_;
        }
    }

    Snapshot snapshot() const {
        std::lock_guard<std::mutex> lk(mtx_);
        Snapshot s;
        s.success = success_;
        s.errors = errors_;
        s.totalLatencyNs = totalLatencyNs_;
        s.maxLatencyMs = maxLatencyMs_;
        s.histogram = histogram_;
        return s;
    }

private:
    mutable std::mutex mtx_;
    int64_t success_{0};
    int64_t errors_{0};
    int64_t totalLatencyNs_{0};
    int64_t maxLatencyMs_;
    std::map<int64_t, int64_t> histogram_;
};

Snapshot diffSnapshot(const Snapshot& before, const Snapshot& after) {
    Snapshot d;
    d.success = after.success - before.success;
    d.errors = after.errors - before.errors;
    d.totalLatencyNs = after.totalLatencyNs - before.totalLatencyNs;
    d.maxLatencyMs = after.maxLatencyMs;
    for (const auto& [bucket, count] : after.histogram) {
        auto it = before.histogram.find(bucket);
        const int64_t delta = count - (it == before.histogram.end() ? 0 : it->second);
        if (delta > 0) d.histogram[bucket] = delta;
    }
    return d;
}

int64_t percentile(const Snapshot& s, double pct) {
    if (s.success <= 0) return 0;
    const int64_t target = static_cast<int64_t>(std::ceil(static_cast<double>(s.success) * pct));
    int64_t cumulative = 0;
    for (const auto& [bucket, count] : s.histogram) {
        cumulative += count;
        if (cumulative >= target) return bucket;
    }
    return s.maxLatencyMs;
}

int64_t maximumLatency(const Snapshot& s) {
    if (s.success <= 0 || s.histogram.empty()) return 0;
    return s.histogram.rbegin()->first;
}

std::string formatStats(double seconds, const Snapshot& s) {
    const int64_t total = s.success + s.errors;
    const double tps = seconds > 0.0 ? static_cast<double>(s.success) / seconds : 0.0;
    const double avgMs = s.success > 0 ? static_cast<double>(s.totalLatencyNs) / 1e6 / static_cast<double>(s.success) : 0.0;
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "duration=%.3fs trxs=%lld succ=%lld err=%lld tps=%.3f avg_ms=%.3f p50=%lld p95=%lld p99=%lld max=%lld",
                  seconds, (long long)total, (long long)s.success, (long long)s.errors, tps, avgMs,
                  (long long)percentile(s, 0.50), (long long)percentile(s, 0.95),
                  (long long)percentile(s, 0.99), (long long)maximumLatency(s));
    return buf;
}

// ---- Leader tracking from reply hints ----

class LeaderTracker {
public:
    LeaderTracker(int n, int f, int initialLeader) : n_(n), f_(f), leader_(initialLeader), hints_(n, {-1, -1}) {}

    int leader() const { return leader_.load(); }

    void observe(int replica, int view, int leaderId) {
        if (replica < 0 || replica >= n_ || leaderId < 0 || leaderId >= n_) return;
        std::lock_guard<std::mutex> lk(mtx_);
        hints_[replica] = {view, leaderId};
        if (view <= adoptedView_) return;
        int agreeing = 0;
        for (const auto& h : hints_) {
            if (h.first == view && h.second == leaderId) ++agreeing;
        }
        if (agreeing >= f_ + 1) {
            adoptedView_ = view;
            const int previous = leader_.exchange(leaderId);
            if (previous != leaderId) {
                LOG_INFO("leader hint adopted: view=" << view << " leader=" << leaderId << " (was " << previous << ")");
            }
        }
    }

private:
    int n_;
    int f_;
    std::atomic<int> leader_;
    std::mutex mtx_;
    int adoptedView_{-1};
    std::vector<std::pair<int, int>> hints_;
};

// ---- Outstanding request tracking ----

class Tracker {
public:
    Tracker(int n, int f, bool broadcast, int requestTimeoutMs, Metrics& metrics)
        : quorum_(f + 1), rejectionQuorum_(broadcast ? n : 1), timeout_(std::chrono::milliseconds(requestTimeoutMs)), metrics_(metrics) {}

    void add(uint64_t requestId, Clock::time_point sent) {
        std::lock_guard<std::mutex> lk(mtx_);
        pending_.emplace(requestId, Pending{sent, 0, 0});
        expiry_.emplace_back(sent + timeout_, requestId);
    }

    void fail(uint64_t requestId) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (pending_.erase(requestId) > 0) {
            metrics_.record(false, std::chrono::nanoseconds(0));
        }
    }

    void onReply(uint64_t requestId, int replica, const std::string& result) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = pending_.find(requestId);
        if (it == pending_.end()) return;
        if (replica < 0 || replica >= 64) return;
        // Local admission rejections are not a consensus decision. Even n-f
        // replicas can reject while another accepts and later proposes the
        // request. Wait for every target to reject, or for execution/deadline.
        if (result != "success") {
            if (it->second.replicas & (uint64_t{1} << replica)) return;
            it->second.rejections |= (uint64_t{1} << replica);
            if (__builtin_popcountll(it->second.rejections) >= rejectionQuorum_) {
                ++rejected;
                metrics_.record(false, std::chrono::nanoseconds(0));
                pending_.erase(it);
            }
            return;
        }
        it->second.rejections &= ~(uint64_t{1} << replica);
        it->second.replicas |= (uint64_t{1} << replica);
        if (__builtin_popcountll(it->second.replicas) >= quorum_) {
            const auto latency = Clock::now() - it->second.sent;
            metrics_.record(true, std::chrono::duration_cast<std::chrono::nanoseconds>(latency));
            pending_.erase(it);
        }
    }

    void sweep(Clock::time_point now) {
        std::lock_guard<std::mutex> lk(mtx_);
        while (!expiry_.empty() && expiry_.front().first <= now) {
            const uint64_t id = expiry_.front().second;
            expiry_.pop_front();
            if (pending_.erase(id) > 0) {
                ++expired;
                metrics_.record(false, std::chrono::nanoseconds(0));
            }
        }
    }

    size_t outstanding() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return pending_.size();
    }

    std::atomic<uint64_t> rejected{0}, expired{0};
private:
    struct Pending {
        Clock::time_point sent;
        uint64_t replicas;
        uint64_t rejections;
    };
    int quorum_;
    int rejectionQuorum_;
    std::chrono::nanoseconds timeout_;
    Metrics& metrics_;
    mutable std::mutex mtx_;
    std::unordered_map<uint64_t, Pending> pending_;
    std::deque<std::pair<Clock::time_point, uint64_t>> expiry_;
};

// ---- Streams ----

struct StreamConn {
    int replica{-1};
    int lane{-1};
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<bedrock::Node::Stub> stub;
    grpc::ClientContext ctx;
    std::unique_ptr<grpc::ClientReaderWriter<bedrock::ClientRequest, bedrock::ClientReply>> rw;
    std::thread reader;
    std::atomic<bool> writable{true};
};

void usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " --committee <path> [options]\n"
        << "  --rate <tx/s>                 total request rate across lanes (default 1000)\n"
        << "  --duration <s>                seconds to run after start (default 0 = until SIGINT)\n"
        << "  --request-timeout <ms>        unreplied requests count as errors after this (default 10000)\n"
        << "  --target-mode <leader|broadcast>  leader: send to the current leader (default); broadcast: to all\n"
        << "  --connections <n>             streams (independent sender lanes) per replica (default 4)\n"
        << "  --start-unix-ms <ms>          epoch ms at which to start submitting\n"
        << "  --monitor-interval <ms>       Monitor line period (default 1000)\n"
        << "  --client-id <prefix>          client id prefix (default client)\n"
        << "  --sign <true|false>           RSA-sign every request with --client-key (default false)\n"
        << "  --client-key <path>           client private key PEM (required with --sign true)\n"
        << "  --initial-leader <id>         leader to target until replies say otherwise (default 0)\n"
        << "  --connect-timeout <s>         give up if a replica is unreachable for this long (default 15)\n";
}

int parseInt(const std::string& flag, const std::string& value) {
    try {
        size_t consumed = 0;
        int parsed = std::stoi(value, &consumed);
        if (consumed != value.size()) throw std::invalid_argument("trailing characters");
        return parsed;
    } catch (const std::exception&) {
        throw std::runtime_error("invalid integer for " + flag + ": '" + value + "'");
    }
}

long long parseLong(const std::string& flag, const std::string& value) {
    try {
        size_t consumed = 0;
        long long parsed = std::stoll(value, &consumed);
        if (consumed != value.size()) throw std::invalid_argument("trailing characters");
        return parsed;
    } catch (const std::exception&) {
        throw std::runtime_error("invalid integer for " + flag + ": '" + value + "'");
    }
}

Options parseOptions(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&](const std::string& flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + flag);
            return argv[++i];
        };
        if (arg == "--committee") o.committeePath = value(arg);
        else if (arg == "--rate") o.rate = static_cast<double>(parseInt(arg, value(arg)));
        else if (arg == "--duration") o.durationS = parseInt(arg, value(arg));
        else if (arg == "--request-timeout") o.requestTimeoutMs = parseInt(arg, value(arg));
        else if (arg == "--target-mode") o.targetMode = value(arg);
        else if (arg == "--connections") o.connections = parseInt(arg, value(arg));
        else if (arg == "--start-unix-ms") o.startUnixMs = parseLong(arg, value(arg));
        else if (arg == "--monitor-interval") o.monitorIntervalMs = parseInt(arg, value(arg));
        else if (arg == "--client-id") o.clientIdPrefix = value(arg);
        else if (arg == "--sign") {
            const std::string v = value(arg);
            if (v == "true") o.sign = true;
            else if (v == "false") o.sign = false;
            else throw std::runtime_error("--sign expects true or false");
        }
        else if (arg == "--client-key") o.clientKeyPath = value(arg);
        else if (arg == "--initial-leader") o.initialLeader = parseInt(arg, value(arg));
        else if (arg == "--connect-timeout") o.connectTimeoutS = parseInt(arg, value(arg));
        else if (arg == "--help" || arg == "-h") { usage(argv[0]); std::exit(0); }
        else throw std::runtime_error("unknown argument: " + arg);
    }
    if (o.committeePath.empty()) throw std::runtime_error("--committee is required");
    if (o.rate <= 0) throw std::runtime_error("--rate must be positive");
    if (o.durationS < 0) throw std::runtime_error("--duration must be >= 0");
    if (o.requestTimeoutMs <= 0) throw std::runtime_error("--request-timeout must be positive");
    if (o.targetMode != "leader" && o.targetMode != "broadcast") throw std::runtime_error("--target-mode must be leader or broadcast");
    if (o.connections <= 0) throw std::runtime_error("--connections must be positive");
    if (o.monitorIntervalMs <= 0) throw std::runtime_error("--monitor-interval must be positive");
    if (o.sign && o.clientKeyPath.empty()) throw std::runtime_error("--client-key is required with --sign true");
    if (o.connectTimeoutS <= 0) throw std::runtime_error("--connect-timeout must be positive");
    return o;
}

}  // namespace

int main(int argc, char** argv) {
    bedrock::logging::setComponent("client");
    Options opt;
    try {
        opt = parseOptions(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        usage(argv[0]);
        return 2;
    }
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGPIPE, SIG_IGN);

    bedrock::Committee committee;
    try {
        committee = bedrock::Committee::loadFromFile(opt.committeePath);
    } catch (const std::exception& e) {
        LOG_ERROR(e.what());
        return 1;
    }
    const int n = committee.size();
    const int f = committee.f();
    if (opt.initialLeader < 0 || opt.initialLeader >= n) {
        LOG_ERROR("--initial-leader " << opt.initialLeader << " is not a replica id (n=" << n << ")");
        return 2;
    }
    if (n > 64) {
        LOG_ERROR("at most 64 replicas are supported by the reply bitmask");
        return 2;
    }

    std::unique_ptr<OpenSSLCryptoProvider> signer;
    if (opt.sign) {
        try {
            signer = std::make_unique<OpenSSLCryptoProvider>(opt.clientKeyPath);
        } catch (const std::exception& e) {
            LOG_ERROR("cannot load client key " << opt.clientKeyPath << ": " << e.what());
            return 1;
        }
    }

    Metrics metrics(opt.requestTimeoutMs);
    Tracker tracker(n, f, opt.targetMode == "broadcast", opt.requestTimeoutMs, metrics);
    LeaderTracker leaders(n, f, opt.initialLeader);
    const int lanes = opt.connections;

    LOG_INFO("committee: n=" << n << " f=" << f << " quorum=" << (f + 1) << " replies per request");
    LOG_INFO("target mode " << opt.targetMode << ": rate=" << opt.rate << " tx/s over " << lanes
             << " lanes (" << (opt.rate / lanes) << " tx/s per lane), request timeout "
             << opt.requestTimeoutMs << " ms, signing " << (opt.sign ? "on" : "off")
             << ", initial leader " << opt.initialLeader);

    // One stream per (lane, replica). Each lane gets its own channel to every
    // replica so lanes are independent TCP connections.
    std::vector<std::vector<std::unique_ptr<StreamConn>>> streams(lanes);
    std::atomic<uint64_t> replies{0};
    std::atomic<uint64_t> readerFailures{0};
    for (int lane = 0; lane < lanes; ++lane) {
        for (int r = 0; r < n; ++r) {
            auto sc = std::make_unique<StreamConn>();
            sc->replica = r;
            sc->lane = lane;
            grpc::ChannelArguments args;
            args.SetInt("bedrock.lane", lane);  // distinct args = distinct connection
            args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
            args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
            args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
            sc->channel = grpc::CreateCustomChannel(committee.replica(r).address(),
                                                    grpc::InsecureChannelCredentials(), args);
            sc->stub = bedrock::Node::NewStub(sc->channel);
            if (!sc->channel->WaitForConnected(std::chrono::system_clock::now() +
                                               std::chrono::seconds(opt.connectTimeoutS))) {
                LOG_ERROR("replica " << r << " (" << committee.replica(r).address() << ") is unreachable after "
                          << opt.connectTimeoutS << " s");
                return 1;
            }
            sc->ctx.set_wait_for_ready(true);
            sc->rw = sc->stub->ClientStream(&sc->ctx);
            bedrock::ClientRequest hello;
            hello.set_client_id(opt.clientIdPrefix + "-" + std::to_string(lane));
            hello.set_request_id(0);
            hello.set_operation("hello");
            if (!sc->rw->Write(hello)) {
                LOG_ERROR("cannot open client stream to replica " << r << " (" << committee.replica(r).address() << ")");
                return 1;
            }
            StreamConn* raw = sc.get();
            sc->reader = std::thread([raw, &tracker, &leaders, &replies, &readerFailures]() {
                bedrock::ClientReply reply;
                while (raw->rw->Read(&reply)) {
                    replies.fetch_add(reply.request_ids_size() ? reply.request_ids_size() : 1);
                    if (reply.result() == "success") leaders.observe(reply.replica_id(), reply.view(), reply.leader_id());
                    if (reply.request_ids_size()) {
                        for (auto id : reply.request_ids()) tracker.onReply(id, reply.replica_id(), reply.result());
                    } else tracker.onReply(reply.request_id(), reply.replica_id(), reply.result());
                }
                raw->writable.store(false);
                const grpc::Status st = raw->rw->Finish();
                if (!st.ok() && !g_stop.load()) {
                    readerFailures.fetch_add(1);
                    LOG_WARN("stream to replica " << raw->replica << " (lane " << raw->lane << ") closed: "
                             << st.error_code() << " " << st.error_message());
                }
            });
            streams[lane].push_back(std::move(sc));
        }
    }
    LOG_INFO("opened " << (lanes * n) << " client streams");

    if (opt.startUnixMs > 0) {
        const long long now = unixMillis();
        const std::time_t startSec = static_cast<std::time_t>(opt.startUnixMs / 1000);
        char iso[64];
        std::tm tm{};
        localtime_r(&startSec, &tm);
        std::strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%S%z", &tm);
        std::cout << "Waiting until benchmark start unix ms " << opt.startUnixMs << " (" << iso << ")\n" << std::flush;
        if (opt.startUnixMs > now) {
            std::this_thread::sleep_for(std::chrono::milliseconds(opt.startUnixMs - now));
        }
    }

    const auto benchmarkStart = Clock::now();
    const auto benchmarkEnd = opt.durationS > 0 ? benchmarkStart + std::chrono::seconds(opt.durationS)
                                                : Clock::time_point::max();
    std::atomic<bool> sendersDone{false};
    std::atomic<uint64_t> sent{0};
    std::atomic<uint64_t> sendFailures{0};

    // Sender lanes.
    std::vector<std::thread> senders;
    for (int lane = 0; lane < lanes; ++lane) {
        senders.emplace_back([&, lane]() {
            const double perTick = opt.rate / lanes / kTicksPerSecond;
            double allowance = 0.0;
            uint64_t counter = 0;
            auto nextTick = Clock::now();
            const std::string clientId = opt.clientIdPrefix + "-" + std::to_string(lane);
            while (!g_stop.load() && Clock::now() < benchmarkEnd) {
                nextTick += std::chrono::milliseconds(1000 / kTicksPerSecond);
                allowance += perTick;
                int toSend = static_cast<int>(allowance);
                allowance -= toSend;
                for (int k = 0; k < toSend && !g_stop.load(); ++k) {
                    const uint64_t requestId = (static_cast<uint64_t>(lane) << kLaneShift) | (++counter);
                    bedrock::ClientRequest req;
                    req.set_client_id(clientId);
                    req.set_request_id(requestId);
                    const std::string op = clientId + ":" + std::to_string(requestId);
                    req.set_timestamp(op);
                    req.set_operation(op);
                    auto* tx = req.mutable_transaction();
                    tx->set_from("acct" + std::to_string(counter % 1000));
                    tx->set_to("acct" + std::to_string((counter + 1) % 1000));
                    tx->set_amount(1);
                    if (signer) {
                        nlohmann::json txj = {{"from", tx->from()}, {"to", tx->to()}, {"amount", tx->amount()}};
                        req.set_signature(signer->sign(txj.dump() + req.timestamp()));
                    }
                    tracker.add(requestId, Clock::now());
                    sent.fetch_add(1);
                    bool delivered = false;
                    if (opt.targetMode == "leader") {
                        StreamConn* target = streams[lane][static_cast<size_t>(leaders.leader())].get();
                        delivered = target->writable.load() && target->rw->Write(req);
                    } else {
                        for (auto& sc : streams[lane]) {
                            if (sc->writable.load() && sc->rw->Write(req)) delivered = true;
                        }
                    }
                    if (!delivered) {
                        sendFailures.fetch_add(1);
                        tracker.fail(requestId);
                    }
                }
                std::this_thread::sleep_until(nextTick);
            }
        });
    }

    // Sweeper: expires unreplied requests.
    std::thread sweeper([&]() {
        while (!sendersDone.load() || tracker.outstanding() > 0) {
            tracker.sweep(Clock::now());
            if (sendersDone.load() && g_stop.load()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    });

    // Monitor: one line per interval.
    std::thread monitor([&]() {
        Snapshot last = metrics.snapshot();
        auto lastTime = Clock::now();
        auto next = lastTime + std::chrono::milliseconds(opt.monitorIntervalMs);
        while (!sendersDone.load()) {
            std::this_thread::sleep_until(next);
            next += std::chrono::milliseconds(opt.monitorIntervalMs);
            const auto now = Clock::now();
            const Snapshot cur = metrics.snapshot();
            const double seconds = std::chrono::duration<double>(now - lastTime).count();
            bedrock::logging::write(bedrock::logging::Level::Info, "Monitor " + formatStats(seconds, diffSnapshot(last, cur)));
            last = cur;
            lastTime = now;
        }
    });

    for (auto& t : senders) t.join();
    sendersDone.store(true);
    LOG_INFO("senders finished: sent=" << sent.load() << " send_failures=" << sendFailures.load()
             << " outstanding=" << tracker.outstanding() << "; draining for up to " << opt.requestTimeoutMs << " ms");
    // Let outstanding requests complete or time out.
    const auto drainDeadline = Clock::now() + std::chrono::milliseconds(opt.requestTimeoutMs);
    while (tracker.outstanding() > 0 && Clock::now() < drainDeadline && !g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    g_stop.store(true);
    tracker.sweep(Clock::time_point::max());
    const auto benchmarkFinished = Clock::now();
    sweeper.join();
    monitor.join();

    const Snapshot final = metrics.snapshot();
    const double totalSeconds = std::chrono::duration<double>(benchmarkFinished - benchmarkStart).count();
    std::cout << "======================================================================\n"
              << "Benchmark results:\n"
              << formatStats(totalSeconds, final) << "\n"
              << "======================================================================\n" << std::flush;
    LOG_INFO("replies received=" << replies.load() << " leader=" << leaders.leader()
             << " stream_failures=" << readerFailures.load()
             << " rejected=" << tracker.rejected.load() << " timed_out=" << tracker.expired.load());

    for (auto& lane : streams) {
        for (auto& sc : lane) {
            sc->rw->WritesDone();
            sc->ctx.TryCancel();
        }
    }
    for (auto& lane : streams) {
        for (auto& sc : lane) {
            if (sc->reader.joinable()) sc->reader.join();
        }
    }
    return 0;
}
