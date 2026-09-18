#pragma once

// A single-threaded delayed task runner. Tasks execute in due-time order on
// one worker thread, so a delayed proposal or an SBFT fast-path wait costs a
// queue entry instead of a thread per request. Tasks must not block for
// long: they normally take the engine mutex, do a small amount of work, and
// return. stop() drops every queued task and joins the worker.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <utility>

namespace bedrock {

class TaskScheduler {
public:
    using Clock = std::chrono::steady_clock;
    using Task = std::function<void()>;

    TaskScheduler() = default;
    ~TaskScheduler();

    TaskScheduler(const TaskScheduler&) = delete;
    TaskScheduler& operator=(const TaskScheduler&) = delete;

    void start();
    void stop();

    // Queues task to run at (or as soon as possible after) due.
    void schedule(Clock::time_point due, Task task);
    void scheduleAfter(std::chrono::milliseconds delay, Task task) {
        schedule(Clock::now() + delay, std::move(task));
    }
    // Drops every queued task; returns how many were dropped. A task that is
    // already running finishes.
    std::size_t cancelAll();
    std::size_t pending() const;
    std::uint64_t executed() const { return executed_.load(); }

private:
    void run();

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::multimap<Clock::time_point, Task> queue_;
    std::uint64_t version_{0};
    bool running_{false};
    std::thread thread_;
    std::atomic<std::uint64_t> executed_{0};
};

}  // namespace bedrock
