#include "core/TaskScheduler.h"

namespace bedrock {

TaskScheduler::~TaskScheduler() {
    stop();
}

void TaskScheduler::start() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (running_) return;
    running_ = true;
    thread_ = std::thread(&TaskScheduler::run, this);
}

void TaskScheduler::stop() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!running_) return;
        running_ = false;
        queue_.clear();
        ++version_;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void TaskScheduler::schedule(Clock::time_point due, Task task) {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        queue_.emplace(due, std::move(task));
        ++version_;
    }
    cv_.notify_all();
}

std::size_t TaskScheduler::cancelAll() {
    std::size_t dropped = 0;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        dropped = queue_.size();
        queue_.clear();
        ++version_;
    }
    cv_.notify_all();
    return dropped;
}

std::size_t TaskScheduler::pending() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return queue_.size();
}

void TaskScheduler::run() {
    std::unique_lock<std::mutex> lk(mtx_);
    while (running_) {
        if (queue_.empty()) {
            cv_.wait(lk, [this] { return !running_ || !queue_.empty(); });
            continue;
        }
        const auto due = queue_.begin()->first;
        const std::uint64_t armedVersion = version_;
        cv_.wait_until(lk, due, [this, armedVersion] { return !running_ || version_ != armedVersion; });
        if (!running_) break;
        if (version_ != armedVersion) continue;
        if (Clock::now() < due) continue;

        Task task = std::move(queue_.begin()->second);
        queue_.erase(queue_.begin());
        lk.unlock();
        task();
        executed_.fetch_add(1);
        lk.lock();
    }
}

}  // namespace bedrock
