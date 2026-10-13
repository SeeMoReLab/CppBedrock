#include "core/PendingRequestTimer.h"

#include "core/Consensus.h"
#include "test_support.h"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using bedrock::PendingRequestTimer;
using Clock = PendingRequestTimer::Clock;
using namespace std::chrono_literals;

namespace {

void watchedRequestIsTimedFromItsArrival() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto old = Clock::now() - 10s;
    CHECK(timer.track("a", old));
    auto a = *timer.watch();
    // The deadline belongs to the request, not to the moment it became watched.
    // One that has already waited longer than the timeout is late as soon as it
    // is picked up; dating the watch to now would instead give every request a
    // full timeout starting from its predecessor's completion, so a leader that
    // orders everything late but steadily would never miss a deadline.
    CHECK(a.since == old);
    CHECK(a.deadline == old + 200ms);
    CHECK(timer.expired(a.generation, Clock::now()));
    CHECK(timer.track("b", old + 1ms));
    CHECK(timer.track("c", old + 2ms));
    CHECK(!timer.track("a"));
    CHECK(timer.complete("c"));
    CHECK(!timer.complete("missing"));
    CHECK(timer.watch()->generation == a.generation);
    CHECK(timer.watch()->deadline == a.deadline);
    CHECK(timer.expired(a.generation, a.deadline));

    CHECK(timer.complete("a"));
    auto b = *timer.watch();
    CHECK_EQ(b.key, std::string("b"));
    CHECK(b.since == old + 1ms);  // its own arrival, not the completion of "a"
    CHECK(b.deadline == b.since + 200ms);
    CHECK(!timer.expired(a.generation, b.deadline));
    CHECK(!timer.expired(b.generation, b.deadline - 1ms));
    CHECK(timer.expired(b.generation, b.deadline));
    CHECK(timer.complete("b"));
    CHECK(!timer.watch());
}

void viewFloorGivesTheNewPrimaryAFullTimeout() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto old = Clock::now() - 10s;
    timer.reset({{"a", old}, {"b", old + 1ms}});
    CHECK(timer.expired(timer.watch()->generation, Clock::now()));
    // Installing a view floors every deadline at the moment it began, so the
    // backlog the old primary left behind cannot expire the new one instantly.
    const auto installed = Clock::now();
    timer.setFloor(installed);
    CHECK(timer.watch()->deadline == installed + 200ms);
    CHECK(!timer.expired(timer.watch()->generation, installed + 199ms));
    CHECK(timer.expired(timer.watch()->generation, installed + 200ms));
    // The head still carries its own arrival, so ordering is untouched, and a
    // request that arrives after the floor is timed from its arrival again.
    CHECK_EQ(timer.watch()->key, std::string("a"));
    CHECK(timer.watch()->floored);
    // The next inherited request is timed from when it became the head, so the
    // primary owes it one timeout of its own rather than sharing a's deadline.
    const auto beforeComplete = Clock::now();
    timer.complete("a");
    CHECK(timer.watch()->floored);
    CHECK(timer.watch()->since >= beforeComplete);
    CHECK(timer.watch()->deadline == timer.watch()->since + 200ms);
    CHECK(timer.watch()->deadline >= installed + 200ms);
    const auto fresh = installed + 10s;
    timer.complete("b");
    CHECK(timer.track("c", fresh));
    CHECK(timer.watch()->deadline == fresh + 200ms);
    CHECK(!timer.watch()->floored);
}

void inheritedBacklogDrainsOneTimeoutPerRequest() {
    // A primary that drains an inherited backlog steadily, one request per
    // 150 ms against a 200 ms timeout, meets every deadline even though the
    // whole backlog takes far longer than one timeout. Under a single shared
    // deadline the third request would already be late.
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto old = Clock::now() - 10s;
    timer.reset({{"a", old}, {"b", old + 1ms}, {"c", old + 2ms}, {"d", old + 3ms}});
    const auto installed = Clock::now();
    timer.setFloor(installed);
    for (const char* key : {"a", "b", "c"}) {
        const auto w = *timer.watch();
        CHECK_EQ(w.key, std::string(key));
        CHECK(!timer.expired(w.generation, Clock::now()));
        std::this_thread::sleep_for(150ms);
        CHECK(!timer.expired(w.generation, Clock::now()));
        timer.complete(key);
    }
    // A request that arrived after the floor is timed from its arrival again,
    // so a primary that is itself late is still caught once the backlog is gone.
    timer.complete("d");
    const auto late = installed + 1ms;  // after the floor, and over 400 ms ago
    CHECK(timer.track("e", late));
    CHECK(timer.watch()->deadline == late + 200ms);
    CHECK(timer.expired(timer.watch()->generation, Clock::now()));
}

void backoffScalesTheDeadlineAndRearms() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto arrived = Clock::now();
    CHECK(timer.track("a", arrived));
    const auto base = *timer.watch();
    CHECK(base.deadline == arrived + 200ms);
    // Each view change a replica starts doubles the timeout, so that the next
    // primary gets more room to clear the backlog than the last one had.
    timer.setBackoff(1);
    CHECK(timer.watch()->deadline == arrived + 400ms);
    CHECK(timer.watch()->generation != base.generation);  // an armed wait re-evaluates
    timer.setBackoff(3);
    CHECK(timer.watch()->deadline == arrived + 1600ms);
    CHECK(!timer.expired(timer.watch()->generation, arrived + 1599ms));
    CHECK(timer.expired(timer.watch()->generation, arrived + 1600ms));
    // Executing the request that was being timed restores the base timeout.
    timer.setBackoff(0);
    CHECK(timer.watch()->deadline == arrived + 200ms);
    // The watch still carries its own arrival, so ordering is untouched.
    CHECK(timer.watch()->since == arrived);
}

void backoffSaturatesRatherThanGrowingWithoutBound() {
    std::atomic<int> timeout{1000};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto arrived = Clock::now();
    CHECK(timer.track("a", arrived));
    timer.setBackoff(4);
    CHECK(timer.watch()->deadline == arrived + 16000ms);
    // A replica must still notice a leader that has stopped, so the doubling
    // stops here rather than running away.
    timer.setBackoff(20);
    CHECK(timer.watch()->deadline == arrived + std::chrono::milliseconds(bedrock::kMaxElectionBackoffMs));
    // A base timeout above the ceiling is honoured, never shortened.
    std::atomic<int> longTimeout{60000};
    PendingRequestTimer slow(longTimeout, [](auto) {});
    slow.track("b", arrived);
    slow.setBackoff(5);
    CHECK(slow.watch()->deadline == arrived + 60000ms);
}

void batchCompletesAtomically() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto old = Clock::now() - 10s;
    timer.reset({{"a", old}, {"b", old + 1ms}, {"seq:1", old + 2ms}, {"c", old + 3ms}});
    const auto generation = timer.watch()->generation;
    CHECK_EQ(timer.complete(std::vector<std::string>{"a", "b", "seq:1", "a"}), 3u);
    CHECK_EQ(timer.watch()->key, std::string("c"));
    CHECK_EQ(timer.watch()->generation, generation + 1);
    CHECK_EQ(timer.size(), 1u);
}

void resetPreservesArrivalOrder() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto old = Clock::now() - 10s;
    timer.track("previous");
    const auto generation = timer.watch()->generation;
    timer.reset({{"later", old + 1ms}, {"oldest", old}, {"last", old + 2ms}});
    CHECK_EQ(timer.watch()->key, std::string("oldest"));
    CHECK(timer.watch()->since == old);  // still timed from when it arrived
    CHECK(!timer.expired(generation, Clock::now() + 1h));
    CHECK(timer.oldestKeys(3) == (std::vector<std::string>{"oldest", "later", "last"}));
    timer.complete("oldest");
    CHECK_EQ(timer.watch()->key, std::string("later"));
}

void timeoutChangesKeepElapsedTime() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    timer.track("a");
    const auto original = *timer.watch();
    timeout = 50;
    timer.timeoutChanged();
    const auto shorter = *timer.watch();
    CHECK(shorter.since == original.since);
    CHECK(shorter.deadline == original.since + 50ms);
    CHECK(timer.expired(shorter.generation, original.since + 60ms));
    CHECK(!timer.expired(original.generation, original.deadline));
    timeout = 400;
    timer.timeoutChanged();
    CHECK(timer.watch()->deadline == original.since + 400ms);
    CHECK(!timer.expired(shorter.generation, original.deadline));
}

// Block delivery as if the callback were waiting for the engine mutex, then
// execute the watched batch. The stale callback must not blame the next head.
void staleCallbackCannotExpireNextWatch() {
    std::atomic<int> timeout{20};
    std::promise<uint64_t> entered;
    std::promise<void> release;
    auto released = release.get_future();
    std::promise<bool> result;
    PendingRequestTimer* self = nullptr;
    PendingRequestTimer timer(timeout, [&](auto generation) {
        entered.set_value(generation);
        released.wait();
        result.set_value(self->expired(generation));
    });
    self = &timer;
    timer.track("a"); timer.track("b");
    timer.start();
    auto callback = entered.get_future();
    CHECK(callback.wait_for(2s) == std::future_status::ready);
    const auto stale = callback.get();
    timeout = 60000;
    timer.timeoutChanged();
    timer.complete("a");
    CHECK(timer.watch()->generation != stale);
    release.set_value();
    auto checked = result.get_future();
    CHECK(checked.wait_for(2s) == std::future_status::ready);
    CHECK(!checked.get());
    timer.stop();
}

void expirationDeliveredOnceAndRearmedOnTimeoutChange() {
    std::atomic<int> timeout{20}, fired{0};
    std::promise<void> first, second;
    PendingRequestTimer timer(timeout, [&](auto) {
        if (++fired == 1) first.set_value();
        else if (fired == 2) second.set_value();
    });
    timer.start();
    std::this_thread::sleep_for(40ms);
    CHECK_EQ(fired.load(), 0);
    timer.track("a");
    CHECK(first.get_future().wait_for(2s) == std::future_status::ready);
    std::this_thread::sleep_for(60ms);
    CHECK_EQ(fired.load(), 1); // No automatic rebasing or busy loop.
    timer.track("b"); timer.complete("b");
    CHECK_EQ(fired.load(), 1);
    timer.timeoutChanged();
    CHECK(second.get_future().wait_for(2s) == std::future_status::ready);
    timer.clear();
    timer.stop();
}

void stopWhileArmed() {
    std::atomic<int> timeout{60000};
    PendingRequestTimer timer(timeout, [](auto) {});
    timer.start(); timer.track("a");
    const auto start = Clock::now();
    timer.stop();
    CHECK(Clock::now() - start < 1s);
}
}

int main() {
    watchedRequestIsTimedFromItsArrival();
    viewFloorGivesTheNewPrimaryAFullTimeout();
    inheritedBacklogDrainsOneTimeoutPerRequest();
    backoffScalesTheDeadlineAndRearms();
    backoffSaturatesRatherThanGrowingWithoutBound();
    batchCompletesAtomically();
    resetPreservesArrivalOrder();
    timeoutChangesKeepElapsedTime();
    staleCallbackCannotExpireNextWatch();
    expirationDeliveredOnceAndRearmedOnTimeoutChange();
    stopWhileArmed();
    return testPassed("pending_request_timer_test");
}
