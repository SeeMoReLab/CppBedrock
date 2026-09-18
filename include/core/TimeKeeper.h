#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

// A repeating timer: once started it fires the callback every timeoutMs
// until reset (restarts the wait) or stopped. The timeout is read from the
// shared atomic at every arm, so a learning-agent recommendation takes
// effect on the next arm without recreating the timer.
class TimeKeeper {
public:
    using Callback = std::function<void()>;

    TimeKeeper(const std::atomic<int>& timeoutMs, Callback cb);
    ~TimeKeeper();

    TimeKeeper(const TimeKeeper&) = delete;
    TimeKeeper& operator=(const TimeKeeper&) = delete;

    void start();
    void reset();
    void stop();

private:
    void run();

    const std::atomic<int>& timeoutMs;
    const Callback callback;
    std::thread timerThread;
    std::atomic<bool> running{false};
    mutable std::mutex mtx;
    std::condition_variable cv;
    bool resetFlag{false};
};
