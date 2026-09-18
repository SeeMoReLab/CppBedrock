#pragma once

// Shared PBFT/SBFT watchdog. Watch one pending entry until it executes;
// then give the oldest remaining entry a fresh timeout. Arrival timestamps
// order entries but never determine the watched entry's deadline.
//
// Keys identify client requests or accepted batch sequences. New-view waits
// have their own scheduler deadline. Expiration is delivered once per arm,
// outside the timer lock. The engine must validate the generation under its
// own lock before acting, since execution can invalidate a queued callback.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace bedrock {

class PendingRequestTimer {
public:
    using Clock = std::chrono::steady_clock;
    using Callback = std::function<void(std::uint64_t)>;
    struct Entry { std::string key; Clock::time_point acceptedAt; };
    struct Watch {
        std::string key;
        Clock::time_point since;
        Clock::time_point deadline;
        std::uint64_t generation;
    };

    PendingRequestTimer(const std::atomic<int>& timeoutMs, Callback onExpire);
    ~PendingRequestTimer();
    PendingRequestTimer(const PendingRequestTimer&) = delete;
    PendingRequestTimer& operator=(const PendingRequestTimer&) = delete;

    void start();
    void stop();
    // Duplicates preserve their original arrival and the current watch.
    bool track(const std::string& key, Clock::time_point acceptedAt = Clock::now());
    bool complete(const std::string& key);
    // Remove a whole executed batch before choosing the next watched entry.
    std::size_t complete(const std::vector<std::string>& keys);
    void clear();
    // Install a pending set with a fresh watch (e.g. after a new view).
    void reset(const std::vector<Entry>& entries);
    // Recompute from watchedSince, without granting a fresh timeout.
    void timeoutChanged();

    bool contains(const std::string& key) const;
    std::size_t size() const;
    std::optional<Watch> watch() const;
    bool expired(std::uint64_t generation, Clock::time_point now = Clock::now()) const;
    std::vector<std::string> oldestKeys(std::size_t n) const;
    std::uint64_t expirations() const { return expirations_.load(); }

private:
    void run();
    void selectWatchLocked();
    Clock::time_point deadlineLocked() const;
    void eraseLocked(const std::string& key, Clock::time_point acceptedAt);

    const std::atomic<int>& timeoutMs_;
    const Callback onExpire_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::map<std::string, Clock::time_point> byKey_;
    std::multimap<Clock::time_point, std::string> byTime_;
    std::optional<std::string> watchedKey_;
    Clock::time_point watchedSince_{};
    std::uint64_t generation_{0};
    bool running_{false};
    std::thread thread_;
    std::atomic<std::uint64_t> expirations_{0};
};

}  // namespace bedrock
