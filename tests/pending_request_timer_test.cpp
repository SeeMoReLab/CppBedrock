#include "core/PendingRequestTimer.h"
#include "test_support.h"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using bedrock::PendingRequestTimer;
using Clock = PendingRequestTimer::Clock;
using namespace std::chrono_literals;

namespace {

void watchedRequestGetsFreshDeadline() {
    std::atomic<int> timeout{200};
    PendingRequestTimer timer(timeout, [](auto) {});
    const auto old = Clock::now() - 10s;
    CHECK(timer.track("a", old));
    auto a = *timer.watch();
    CHECK(a.since > old);
    CHECK(a.deadline == a.since + 200ms);
    CHECK(timer.track("b", old + 1ms));
    CHECK(timer.track("c", old + 2ms));
    CHECK(!timer.track("a"));
    CHECK(timer.complete("c"));
    CHECK(!timer.complete("missing"));
    CHECK(timer.watch()->generation == a.generation);
    CHECK(timer.watch()->deadline == a.deadline);
    CHECK(timer.expired(a.generation, a.deadline));

    const auto before = Clock::now();
    CHECK(timer.complete("a"));
    auto b = *timer.watch();
    CHECK_EQ(b.key, std::string("b"));
    CHECK(b.since >= before);
    CHECK(b.deadline == b.since + 200ms);
    CHECK(!timer.expired(a.generation, b.deadline));
    CHECK(!timer.expired(b.generation, b.deadline - 1ms));
    CHECK(timer.expired(b.generation, b.deadline));
    CHECK(timer.complete("b"));
    CHECK(!timer.watch());
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
    CHECK(timer.watch()->since >= old + 10s);
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
    watchedRequestGetsFreshDeadline();
    batchCompletesAtomically();
    resetPreservesArrivalOrder();
    timeoutChangesKeepElapsedTime();
    staleCallbackCannotExpireNextWatch();
    expirationDeliveredOnceAndRearmedOnTimeoutChange();
    stopWhileArmed();
    return testPassed("pending_request_timer_test");
}
