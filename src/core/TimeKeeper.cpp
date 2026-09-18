#include "core/TimeKeeper.h"

#include <algorithm>

TimeKeeper::TimeKeeper(const std::atomic<int>& timeoutMs, Callback cb)
    : timeoutMs(timeoutMs), callback(std::move(cb)) {}

TimeKeeper::~TimeKeeper() {
    stop();
}

void TimeKeeper::start() {
    std::lock_guard<std::mutex> lock(mtx);
    if (running) return;
    running = true;
    timerThread = std::thread(&TimeKeeper::run, this);
}

void TimeKeeper::reset() {
    std::lock_guard<std::mutex> lock(mtx);
    if (!running) return;
    resetFlag = true;
    cv.notify_one();
}

void TimeKeeper::stop() {
    bool should_join = false;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (!running) return;
        running = false;
        resetFlag = true;
        cv.notify_all();
        should_join = timerThread.joinable();
    }
    if (should_join) {
        timerThread.join();
    }
}

void TimeKeeper::run() {
    Callback cbCopy = callback;
    std::unique_lock<std::mutex> lock(mtx);

    while (running) {
        resetFlag = false;
        const int waitMs = std::max(1, timeoutMs.load());
        auto status = cv.wait_for(lock,
            std::chrono::milliseconds(waitMs),
            [this] { return resetFlag || !running; });

        if (!running) break;
        if (status) continue;  // reset: re-arm with the current timeout

        if (running && cbCopy) {
            lock.unlock();
            // Offload the callback so a slow handler never blocks the timer.
            std::thread([cbCopy]() {
                try {
                    cbCopy();
                } catch (const std::exception&) {
                }
            }).detach();
            lock.lock();
            if (!running) break;
        }
    }
}
