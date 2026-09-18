// Unit test for bedrock::TaskScheduler.

#include "core/TaskScheduler.h"
#include "test_support.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

using bedrock::TaskScheduler;
using Clock = TaskScheduler::Clock;
using namespace std::chrono_literals;

namespace {

void runsInDueOrder() {
    TaskScheduler sched;
    sched.start();
    std::mutex mtx;
    std::vector<int> order;
    const auto now = Clock::now();
    sched.schedule(now + 120ms, [&] { std::lock_guard<std::mutex> lk(mtx); order.push_back(3); });
    sched.schedule(now + 40ms, [&] { std::lock_guard<std::mutex> lk(mtx); order.push_back(1); });
    sched.schedule(now + 80ms, [&] { std::lock_guard<std::mutex> lk(mtx); order.push_back(2); });
    std::this_thread::sleep_for(300ms);
    std::lock_guard<std::mutex> lk(mtx);
    CHECK_EQ(order.size(), std::size_t{3});
    CHECK_EQ(order[0], 1);
    CHECK_EQ(order[1], 2);
    CHECK_EQ(order[2], 3);
    sched.stop();
}

void honorsDelay() {
    TaskScheduler sched;
    sched.start();
    std::atomic<bool> ran{false};
    Clock::time_point ranAt{};
    const auto t0 = Clock::now();
    sched.scheduleAfter(150ms, [&] { ranAt = Clock::now(); ran = true; });
    std::this_thread::sleep_for(50ms);
    CHECK(!ran.load());
    std::this_thread::sleep_for(250ms);
    CHECK(ran.load());
    CHECK(ranAt - t0 >= 150ms);
    sched.stop();
}

void cancelAllDropsQueuedTasks() {
    TaskScheduler sched;
    sched.start();
    std::atomic<int> ran{0};
    sched.scheduleAfter(100ms, [&] { ran.fetch_add(1); });
    sched.scheduleAfter(120ms, [&] { ran.fetch_add(1); });
    CHECK_EQ(sched.cancelAll(), std::size_t{2});
    std::this_thread::sleep_for(250ms);
    CHECK_EQ(ran.load(), 0);
    CHECK_EQ(sched.pending(), std::size_t{0});
    sched.stop();
}

void stopIsPrompt() {
    TaskScheduler sched;
    sched.start();
    sched.scheduleAfter(60s, [] {});
    const auto t0 = Clock::now();
    sched.stop();
    CHECK(Clock::now() - t0 < 1s);
    CHECK_EQ(sched.pending(), std::size_t{0});
}

void manyTasksOneThread() {
    TaskScheduler sched;
    sched.start();
    std::atomic<int> ran{0};
    const auto now = Clock::now();
    for (int i = 0; i < 2000; ++i) {
        sched.schedule(now + std::chrono::milliseconds(i % 50), [&] { ran.fetch_add(1); });
    }
    std::this_thread::sleep_for(400ms);
    CHECK_EQ(ran.load(), 2000);
    CHECK_EQ(sched.executed(), std::uint64_t{2000});
    sched.stop();
}

}  // namespace

int main() {
    runsInDueOrder();
    honorsDelay();
    cancelAllDropsQueuedTasks();
    stopIsPrompt();
    manyTasksOneThread();
    return testPassed("task_scheduler_test");
}
