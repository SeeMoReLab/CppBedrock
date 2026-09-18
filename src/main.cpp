// CppBedrock replica entry point: one replica per process.
//
//   CppBedrock --node-id 0 --committee netcfg/committee.json \
//              --protocol-config config/config.pbft.yaml --keys-dir netcfg/keys \
//              [--agent-port 15501 --learning-feature-duration 10000 ...] \
//              [--initial-timeout 5000] [--initial-slow-path-timeout 20] \
//              [--failure-spec failure_spec.xml --failure-start-unix-ms 1700000000000] \
//              [--stats-interval 1000] [--verbose]
//
// The process runs until SIGINT/SIGTERM.

#include "core/Entity.h"
#include "core/Log.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

std::mutex g_stopMtx;
std::condition_variable g_stopCv;
std::atomic<bool> g_stopRequested{false};

void onSignal(int) {
    g_stopRequested.store(true);
    g_stopCv.notify_all();
}

void usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " --node-id <int> --committee <path> --protocol-config <path> --keys-dir <path>\n"
        << "  [--agent-port <port>]                 learning-agent gRPC port on 127.0.0.1 (0 = no agent)\n"
        << "  [--learning-feature-duration <ms>]    default 10000\n"
        << "  [--learning-reply-wait <ms>]          default 2000\n"
        << "  [--learning-warmup-duration <ms>]     default 3000\n"
        << "  [--learning-reward-duration <ms>]     default 5000\n"
        << "  [--initial-timeout <ms>]              election (view-change) timeout; default from the protocol config\n"
        << "  [--initial-slow-path-timeout <ms>]    SBFT fast-path wait; default from the protocol config\n"
        << "  [--failure-spec <path>]               shared failure_spec.xml; only this protocol's section is read\n"
        << "  [--failure-start-unix-ms <ms>]        experiment start anchoring the failure spec phases (required with --failure-spec)\n"
        << "  [--stats-interval <ms>]               period of the Stats log line (default 1000, 0 = off)\n"
        << "  [--proposal-interval <ms>]           fixed batch cadence (default 100)\n"
        << "  [--batch-max-requests <count>]       requests per batch (default/max 8192)\n"
        << "  [--batch-max-bytes <bytes>]          batch bytes (default/max 524288)\n"
        << "  [--max-inflight-batches <count>]     unexecuted batches (default and max 16, the consensus window)\n"
        << "  [--max-pending-requests <count>]     bound each ingress/pending queue (default 32768)\n"
        << "  [--max-pending-bytes <bytes>]        bound each ingress/pending queue (default 16777216)\n"
        << "  [--proposal-signing <true|false>]     Required true for PBFT/SBFT (default true)\n"
        << "  [--verbose]                           emit per-message debug logs\n";
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

std::uint64_t parseUnsigned(const std::string& flag, const std::string& value) {
    try {
        size_t consumed = 0;
        const unsigned long long parsed = std::stoull(value, &consumed);
        if (consumed != value.size() || value.empty() || value[0] == '-') throw std::invalid_argument("bad");
        return static_cast<std::uint64_t>(parsed);
    } catch (const std::exception&) {
        throw std::runtime_error("invalid unsigned integer for " + flag + ": '" + value + "'");
    }
}

}  // namespace

int main(int argc, char** argv) {
    EntityOptions options;
    bool verbose = false;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto value = [&](const std::string& flag) -> std::string {
                if (i + 1 >= argc) throw std::runtime_error("missing value for " + flag);
                return argv[++i];
            };
            if (arg == "--node-id") options.nodeId = parseInt(arg, value(arg));
            else if (arg == "--committee") options.committeePath = value(arg);
            else if (arg == "--protocol-config") options.protocolConfigPath = value(arg);
            else if (arg == "--keys-dir") options.keysDir = value(arg);
            else if (arg == "--agent-port") options.agentPort = parseInt(arg, value(arg));
            else if (arg == "--learning-feature-duration") options.agentConfig.featureDurationMs = parseInt(arg, value(arg));
            else if (arg == "--learning-reply-wait") options.agentConfig.replyWaitMs = parseInt(arg, value(arg));
            else if (arg == "--learning-warmup-duration") options.agentConfig.warmupDurationMs = parseInt(arg, value(arg));
            else if (arg == "--learning-reward-duration") options.agentConfig.rewardDurationMs = parseInt(arg, value(arg));
            else if (arg == "--initial-timeout") options.initialElectionTimeoutMs = parseInt(arg, value(arg));
            else if (arg == "--initial-slow-path-timeout") options.initialSlowPathTimeoutMs = parseInt(arg, value(arg));
            else if (arg == "--failure-spec") options.failureSpecPath = value(arg);
            else if (arg == "--failure-start-unix-ms") options.failureStartUnixMs = parseUnsigned(arg, value(arg));
            else if (arg == "--stats-interval") options.statsIntervalMs = parseInt(arg, value(arg));
            else if (arg == "--proposal-interval") options.proposalIntervalMs = parseInt(arg, value(arg));
            else if (arg == "--batch-max-requests") options.batchMaxRequests = parseInt(arg, value(arg));
            else if (arg == "--batch-max-bytes") options.batchMaxBytes = parseInt(arg, value(arg));
            else if (arg == "--max-inflight-batches") options.maxInflightBatches = parseInt(arg, value(arg));
            else if (arg == "--max-pending-requests") options.maxPendingRequests = parseInt(arg, value(arg));
            else if (arg == "--max-pending-bytes") options.maxPendingBytes = parseInt(arg, value(arg));
            else if (arg == "--proposal-signing") {
                const std::string v = value(arg);
                if (v == "true") options.proposalSigning = true;
                else if (v == "false") options.proposalSigning = false;
                else throw std::runtime_error("--proposal-signing expects true or false");
            }
            else if (arg == "--verbose" || arg == "-v") verbose = true;
            else if (arg == "--help" || arg == "-h") { usage(argv[0]); return 0; }
            else throw std::runtime_error("unknown argument: " + arg);
        }
        if (options.nodeId < 0) throw std::runtime_error("--node-id is required");
        if (options.committeePath.empty()) throw std::runtime_error("--committee is required");
        if (options.protocolConfigPath.empty()) throw std::runtime_error("--protocol-config is required");
        if (options.keysDir.empty()) throw std::runtime_error("--keys-dir is required");
        if (options.agentPort < 0) throw std::runtime_error("--agent-port must be >= 0");
        if (options.statsIntervalMs < 0) throw std::runtime_error("--stats-interval must be >= 0");
        if (options.failureSpecPath.empty() != (options.failureStartUnixMs == 0)) {
            throw std::runtime_error("--failure-spec and --failure-start-unix-ms must be given together");
        }
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        usage(argv[0]);
        return 2;
    }

    bedrock::logging::setComponent("server_" + std::to_string(options.nodeId));
    bedrock::logging::setLevel(verbose ? bedrock::logging::Level::Debug : bedrock::logging::Level::Info);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGPIPE, SIG_IGN);

    try {
        Entity replica(options);
        replica.start();
        {
            std::unique_lock<std::mutex> lk(g_stopMtx);
            g_stopCv.wait(lk, [] { return g_stopRequested.load(); });
        }
        LOG_INFO("signal received; shutting down");
        replica.stop();
    } catch (const std::exception& e) {
        LOG_ERROR("fatal: " << e.what());
        return 1;
    }
    return 0;
}
