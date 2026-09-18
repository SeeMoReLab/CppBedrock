#pragma once

// Protocol-specific failure injection: the proposal-delay controller, a port
// of SmartBFT/examples/smallbank/failure.go and autobahn/adaptive/src/failure.rs.
// It reads one protocol section of the shared failure-spec XML (for example
// <pbft><proposalDelay> or <sbft><proposalDelay>), anchored to the
// harness-wide --failure-start-unix-ms timestamp, and answers "how long must
// this replica delay its proposal right now". The <network> and <agent>
// sections are applied by other components and are ignored here.
//
// Semantics mirrored from the references:
// - warmUpTime / warmUpTimeMs shifts every phase.
// - A phase starts at startAtMs / atTimeMs / startAt / atTime / time (first
//   present wins, in that order); phases sort by start, then document order.
// - interval / intervalMs repeats a phase every interval until the next
//   phase's start (or count repetitions). With <id>leader</id> the leader
//   window is re-resolved (pinned) once per interval tick.
// - The leader window is the leader plus the next (n-1)/3 - 1 replicas in
//   ascending id order (window size f).
// - Explicit replica ids take precedence over the leader-window rule.
// - Replica ids are global 0-based ids.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace bedrock {

class ProposalDelayController {
public:
    struct Rule {
        std::map<int, std::chrono::milliseconds> replicaDelays;
        std::chrono::milliseconds leaderWindowDelay{0};
        bool hasLeaderWindowRule{false};
    };
    struct Phase {
        std::chrono::milliseconds startOffset{0};
        std::chrono::milliseconds interval{0};
        std::size_t order{0};
        Rule rule;
    };

    // A controller that never delays.
    ProposalDelayController() = default;

    // Parses the spec; throws std::runtime_error on malformed XML or an
    // invalid replica id. A spec without the requested section yields a
    // controller with no delays.
    static std::unique_ptr<ProposalDelayController> parse(const std::string& xml,
                                                          std::uint64_t startUnixMs,
                                                          const std::string& section);
    static std::unique_ptr<ProposalDelayController> loadFile(const std::string& path,
                                                             std::uint64_t startUnixMs,
                                                             const std::string& section);

    bool enabled() const { return enabled_; }
    std::chrono::milliseconds warmUp() const { return warmUp_; }
    const std::vector<Phase>& phases() const { return phases_; }

    // Observe the current leader; pins the leader window for the active
    // phase/interval tick when a leader-window rule is active.
    void observeLeader(int leader, const std::vector<int>& replicaIds);
    // The delay for this replica from the active phase's explicit rules and
    // the already-pinned leader window, without re-resolving the leader.
    std::chrono::milliseconds delayFor(int replicaId);
    // The delay this replica must add before its proposal right now.
    std::chrono::milliseconds delayForProposal(int replicaId, int leader, const std::vector<int>& replicaIds);

    static std::uint64_t nowUnixMs();

private:
    bool elapsedSinceWarmup(std::chrono::milliseconds& out) const;
    long activePhase(bool haveElapsed, std::chrono::milliseconds elapsed) const;
    std::int64_t pinKey(long phase, std::chrono::milliseconds elapsed, std::int64_t& tick) const;
    void logPhaseChange(long phase);
    void pinLeaderWindow(long phase, std::int64_t key, std::int64_t tick, int leader,
                         const std::vector<int>& replicaIds);
    std::chrono::milliseconds pinnedDelay(std::int64_t key, int replicaId) const;

    bool enabled_{false};
    std::uint64_t startUnixMs_{0};
    std::chrono::milliseconds warmUp_{0};
    std::vector<Phase> phases_;
    std::atomic<long> lastLoggedPhase_{-2};
    mutable std::mutex mtx_;
    std::map<std::int64_t, std::map<int, std::chrono::milliseconds>> pinned_;
};

}  // namespace bedrock
