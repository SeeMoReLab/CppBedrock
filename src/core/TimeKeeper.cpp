#include "../../include/core/TimeKeeper.h"
#include <iostream>

TimeKeeper::TimeKeeper(int timeoutMs, Callback cb)
    : timeoutMs(timeoutMs), callback(cb) {}

TimeKeeper::~TimeKeeper() {
    stop();
}

void TimeKeeper::start() {
    // std::cout << "[TimeKeeper] Starting timer with timeout: " << timeoutMs << " ms" << std::endl;   
    std::lock_guard<std::mutex> lock(mtx);
    if (running) return;
    running = true;
    timerThread = std::thread(&TimeKeeper::run, this);
}

void TimeKeeper::reset() {
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (!running) return;
        resetFlag = true;
        cv.notify_one();
    }
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

        auto status = cv.wait_for(lock,
            std::chrono::milliseconds(timeoutMs),
            [this] { return resetFlag || !running; });

        if (!running) break;
        if (status) continue;

        if (running && cbCopy) {
            lock.unlock();
            try { cbCopy(); } catch (...) {}
            lock.lock();
            if (!running) break;
        }
    }
}