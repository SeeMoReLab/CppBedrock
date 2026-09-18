// Unit test for bedrock::ProposalDelayController, mirroring the tests of the
// Rust port (autobahn/adaptive/src/failure.rs) against a <pbft> section.

#include "core/FailureSpec.h"
#include "test_support.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using bedrock::ProposalDelayController;
using namespace std::chrono_literals;
using ms = std::chrono::milliseconds;

namespace {

const char* kSpec = R"(<?xml version="1.0"?>
<failureSpec>
    <schemaVersion>4</schemaVersion>
    <replicaIdType>global</replicaIdType>
    <warmUpTime>10</warmUpTime>
    <globalNetworkDelay><delayMs>5</delayMs></globalNetworkDelay>
    <phases>
        <phase>
            <atTime>0</atTime>
            <network>
                <directedEdges>true</directedEdges>
                <global><delayMs>0</delayMs><burstDurationMs>0</burstDurationMs><burstIntervalMs>0</burstIntervalMs></global>
            </network>
            <pbft>
                <proposalDelay>
                    <replicas></replicas>
                </proposalDelay>
            </pbft>
            <agent><dirtyReport><replicas></replicas></dirtyReport></agent>
        </phase>
        <phase>
            <atTime>60</atTime>
            <pbft>
                <proposalDelay>
                    <interval>30</interval>
                    <replicas>
                        <replica><id>leader</id><delayMs>3000</delayMs></replica>
                    </replicas>
                </proposalDelay>
            </pbft>
        </phase>
        <phase>
            <atTime>120</atTime>
            <pbft>
                <proposalDelay>
                    <replicas>
                        <replica><id>2</id><delayMs>500</delayMs></replica>
                    </replicas>
                </proposalDelay>
            </pbft>
        </phase>
    </phases>
</failureSpec>)";

// A controller positioned "offset" past the end of warm-up.
std::unique_ptr<ProposalDelayController> controllerAt(ms offsetFromWarmup, const std::string& section = "pbft") {
    const std::uint64_t start = ProposalDelayController::nowUnixMs() - 10'000 - static_cast<std::uint64_t>(offsetFromWarmup.count());
    return ProposalDelayController::parse(kSpec, start, section);
}

std::vector<int> ids(int n) {
    std::vector<int> v;
    for (int i = 0; i < n; ++i) v.push_back(i);
    return v;
}

void parsesRealSpecShape() {
    auto ctrl = ProposalDelayController::parse(kSpec, ProposalDelayController::nowUnixMs(), "pbft");
    // Phase 1 repeats every 30s until phase 2 at 120s: entries at 60 and 90,
    // plus phase 0 and phase 2.
    CHECK_EQ(ctrl->phases().size(), std::size_t{4});
    CHECK_EQ(ctrl->warmUp().count(), 10'000);
    CHECK(ctrl->phases()[1].rule.hasLeaderWindowRule);
    CHECK_EQ(ctrl->phases()[1].rule.leaderWindowDelay.count(), 3000);
    CHECK_EQ(ctrl->phases()[2].startOffset.count(), 90'000);
    CHECK_EQ(ctrl->phases()[3].rule.replicaDelays.at(2).count(), 500);
}

void warmupDisablesInjection() {
    auto ctrl = ProposalDelayController::parse(kSpec, ProposalDelayController::nowUnixMs(), "pbft");
    CHECK_EQ(ctrl->delayForProposal(0, 0, ids(4)).count(), 0);
}

void leaderWindowTargetsLeaderAndSuccessors() {
    auto ctrl = controllerAt(100s);  // inside the leader-delay phase (60..120)
    const auto all = ids(7);         // n=7 -> f=2 -> window {leader, leader+1}
    CHECK_EQ(ctrl->delayForProposal(3, 3, all).count(), 3000);
    CHECK_EQ(ctrl->delayForProposal(4, 3, all).count(), 3000);
    CHECK_EQ(ctrl->delayForProposal(5, 3, all).count(), 0);
    CHECK_EQ(ctrl->delayForProposal(2, 3, all).count(), 0);
}

void leaderWindowPinsWithinIntervalTick() {
    auto ctrl = controllerAt(100s);
    const auto all = ids(4);  // n=4 -> f=1 -> window {leader}
    CHECK_EQ(ctrl->delayForProposal(1, 1, all).count(), 3000);
    // A later leader change within the same tick does not re-pin.
    CHECK_EQ(ctrl->delayForProposal(2, 2, all).count(), 0);
    CHECK_EQ(ctrl->delayForProposal(1, 2, all).count(), 3000);
}

void explicitReplicaRuleApplies() {
    auto ctrl = controllerAt(200s);  // phase at 120s
    const auto all = ids(4);
    CHECK_EQ(ctrl->delayForProposal(2, 0, all).count(), 500);
    CHECK_EQ(ctrl->delayForProposal(0, 0, all).count(), 0);
}

void disabledControllerReturnsZero() {
    ProposalDelayController ctrl;
    CHECK(!ctrl.enabled());
    CHECK_EQ(ctrl.delayForProposal(0, 0, ids(4)).count(), 0);
}

void missingSectionYieldsNoDelays() {
    auto sbft = controllerAt(100s, "sbft");
    CHECK_EQ(sbft->delayForProposal(1, 1, ids(4)).count(), 0);
    auto pbft = controllerAt(100s, "pbft");
    CHECK(pbft->delayForProposal(1, 1, ids(4)).count() != 0);
}

void delayForUsesPinnedWindowOnly() {
    auto ctrl = controllerAt(100s);
    const auto all = ids(4);
    CHECK_EQ(ctrl->delayFor(1).count(), 0);  // nothing pinned yet
    ctrl->observeLeader(1, all);
    CHECK_EQ(ctrl->delayFor(1).count(), 3000);
    CHECK_EQ(ctrl->delayFor(0).count(), 0);
}

void idListWithDefaultDelay() {
    const std::string spec = R"(<failureSpec><warmUpTimeMs>0</warmUpTimeMs><phases><phase><atTimeMs>0</atTimeMs>
        <sbft><proposalDelay><delayMs>250</delayMs><replicas><id>1</id><id>leader</id></replicas></proposalDelay></sbft>
        </phase></phases></failureSpec>)";
    auto ctrl = ProposalDelayController::parse(spec, ProposalDelayController::nowUnixMs() - 1000, "sbft");
    CHECK_EQ(ctrl->phases().size(), std::size_t{1});
    CHECK_EQ(ctrl->phases()[0].rule.replicaDelays.at(1).count(), 250);
    CHECK(ctrl->phases()[0].rule.hasLeaderWindowRule);
    CHECK_EQ(ctrl->delayForProposal(1, 0, ids(4)).count(), 250);
    CHECK_EQ(ctrl->delayForProposal(0, 0, ids(4)).count(), 250);
    CHECK_EQ(ctrl->delayForProposal(3, 0, ids(4)).count(), 0);
}

void malformedInputIsAnError() {
    bool threw = false;
    try {
        ProposalDelayController::parse("<failureSpec><phases><phase", 0, "pbft");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    threw = false;
    try {
        ProposalDelayController::parse(
            "<failureSpec><phases><phase><pbft><proposalDelay><replicas><replica><id>x</id><delayMs>1</delayMs></replica></replicas></proposalDelay></pbft></phase></phases></failureSpec>",
            0, "pbft");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    threw = false;
    try {
        ProposalDelayController::loadFile("/nonexistent/failure_spec.xml", 0, "pbft");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

}  // namespace

int main() {
    parsesRealSpecShape();
    warmupDisablesInjection();
    leaderWindowTargetsLeaderAndSuccessors();
    leaderWindowPinsWithinIntervalTick();
    explicitReplicaRuleApplies();
    disabledControllerReturnsZero();
    missingSectionYieldsNoDelays();
    delayForUsesPinnedWindowOnly();
    idListWithDefaultDelay();
    malformedInputIsAnError();
    return testPassed("failure_spec_test");
}
