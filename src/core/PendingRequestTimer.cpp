#include "core/PendingRequestTimer.h"

#include <algorithm>

namespace bedrock {

PendingRequestTimer::PendingRequestTimer(const std::atomic<int>& timeoutMs, Callback onExpire)
    : timeoutMs_(timeoutMs), onExpire_(std::move(onExpire)) {}

PendingRequestTimer::~PendingRequestTimer() { stop(); }

void PendingRequestTimer::start() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (running_) return;
    running_ = true;
    thread_ = std::thread(&PendingRequestTimer::run, this);
}

void PendingRequestTimer::stop() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!running_) return;
        running_ = false;
        ++generation_;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void PendingRequestTimer::selectWatchLocked() {
    ++generation_;
    watchedKey_.reset();
    if (byTime_.empty()) return;
    watchedKey_ = byTime_.begin()->second;
    watchedSince_ = Clock::now();
}

PendingRequestTimer::Clock::time_point PendingRequestTimer::deadlineLocked() const {
    return watchedSince_ + std::chrono::milliseconds(std::max(1, timeoutMs_.load()));
}

bool PendingRequestTimer::track(const std::string& key, Clock::time_point acceptedAt) {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!byKey_.emplace(key, acceptedAt).second) return false;
        byTime_.emplace(acceptedAt, key);
        if (!watchedKey_) selectWatchLocked();
    }
    cv_.notify_all();
    return true;
}

void PendingRequestTimer::eraseLocked(const std::string& key, Clock::time_point acceptedAt) {
    auto range = byTime_.equal_range(acceptedAt);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == key) { byTime_.erase(it); break; }
    }
    byKey_.erase(key);
}

bool PendingRequestTimer::complete(const std::string& key) {
    return complete(std::vector<std::string>{key}) != 0;
}

std::size_t PendingRequestTimer::complete(const std::vector<std::string>& keys) {
    std::size_t removed = 0;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const auto& key : keys) {
            auto it = byKey_.find(key);
            if (it == byKey_.end()) continue;
            eraseLocked(key, it->second);
            ++removed;
        }
        if (watchedKey_ && !byKey_.count(*watchedKey_)) selectWatchLocked();
    }
    cv_.notify_all();
    return removed;
}

void PendingRequestTimer::clear() { reset({}); }

void PendingRequestTimer::reset(const std::vector<Entry>& entries) {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        byKey_.clear();
        byTime_.clear();
        for (const auto& entry : entries) {
            if (byKey_.emplace(entry.key, entry.acceptedAt).second)
                byTime_.emplace(entry.acceptedAt, entry.key);
        }
        selectWatchLocked();
    }
    cv_.notify_all();
}

void PendingRequestTimer::timeoutChanged() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        ++generation_;
    }
    cv_.notify_all();
}

bool PendingRequestTimer::contains(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mtx_);
    return byKey_.count(key) != 0;
}

std::size_t PendingRequestTimer::size() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return byKey_.size();
}

std::optional<PendingRequestTimer::Watch> PendingRequestTimer::watch() const {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!watchedKey_) return std::nullopt;
    return Watch{*watchedKey_, watchedSince_, deadlineLocked(), generation_};
}

bool PendingRequestTimer::expired(std::uint64_t generation, Clock::time_point now) const {
    std::lock_guard<std::mutex> lk(mtx_);
    return watchedKey_ && generation == generation_ && now >= deadlineLocked();
}

std::vector<std::string> PendingRequestTimer::oldestKeys(std::size_t n) const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::vector<std::string> out;
    for (auto it = byTime_.begin(); it != byTime_.end() && out.size() < n; ++it) out.push_back(it->second);
    return out;
}

void PendingRequestTimer::run() {
    std::unique_lock<std::mutex> lk(mtx_);
    std::optional<std::uint64_t> delivered;
    while (running_) {
        if (!watchedKey_ || delivered == generation_) {
            cv_.wait(lk, [this, &delivered] { return !running_ || (watchedKey_ && delivered != generation_); });
            continue;
        }
        const auto deadline = deadlineLocked();
        const auto armedGeneration = generation_;
        cv_.wait_until(lk, deadline, [this, armedGeneration] { return !running_ || generation_ != armedGeneration; });
        if (!running_) break;
        if (generation_ != armedGeneration || Clock::now() < deadlineLocked()) continue;
        delivered = armedGeneration;
        expirations_.fetch_add(1);
        lk.unlock();
        onExpire_(armedGeneration);
        lk.lock();
    }
}

}  // namespace bedrock
