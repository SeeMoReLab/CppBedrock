// Live smoke test for the protocol-agnostic learning-agent client.
//
// Requires a learning agent listening on 127.0.0.1:<port> (default 50190):
//   python3 learning_agent/main.py --node-id 1 --protocol pbft \
//     --host 127.0.0.1 --port 50190 --signing false --model-type random
//
// Usage: agent_client_smoke [protocol] [port] [seconds]
//   protocol: PBFT | LinearPBFT | SBFT | Hotstuff | Hotstuff2 |
//             ChainedHotstuff | Zyzzyva (default PBFT)

#include "core/agent/AgentClient.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>
#include <cstdint>
#include <cstdlib>
#include <string>

int main(int argc, char** argv) {
    std::string protocol = argc > 1 ? argv[1] : "PBFT";
    int port = argc > 2 ? std::atoi(argv[2]) : 50190;
    int seconds = argc > 3 ? std::atoi(argv[3]) : 12;

    AgentClientConfig cfg;
    cfg.nodeId = 1;
    cfg.port = port;
    cfg.featureDurationMs = 2000;
    cfg.replyWaitMs = 1000;
    cfg.warmupDurationMs = 500;
    cfg.rewardDurationMs = 1500;

    std::atomic<int> applies{0};
    AgentClient client(
        cfg, makeProtocolAgentAdapter(protocol), AgentTimeouts{8000, 20},
        [&applies](const AgentTimeouts& t) {
            ++applies;
            std::cout << "[smoke] applied election=" << t.electionMs
                      << "ms slow_path=" << t.slowPathMs << "ms\n";
        });
    client.start();

    std::mt19937 rng(42);
    std::uniform_int_distribution<long long> lat(5000, 40000);
    auto end = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    uint32_t seq = 0;
    while (std::chrono::steady_clock::now() < end) {
        AgentConsensusSample s;
        s.sequence = ++seq;
        s.latencyUs = lat(rng);
        s.phase1Us = lat(rng) / 4;
        s.phase2Us = lat(rng) / 4;
        s.phase3Us = lat(rng) / 4;
        client.recordConsensus(s);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    client.stop();

    std::cout << "[smoke] done: consensus_samples=" << seq
              << " applies=" << applies.load() << "\n";
    if (applies.load() == 0) {
        std::cerr << "[smoke] FAILED: no recommendation was applied\n";
        return 1;
    }
    std::cout << "[smoke] PASSED\n";
    return 0;
}
