#pragma once

#include <charconv>
#include <limits>
#include <map>
#include <string>
#include <stdexcept>
#include <nlohmann/json.hpp>

namespace bedrock {
// Exact replay protection, compressed into disjoint inclusive ID intervals
// per client. Holes remain holes; out-of-order requests are never discarded.
// Sequential benchmark clients need constant-size checkpoint replay state.
class ExecutedRequests {
    using Ranges = std::map<uint64_t, uint64_t>;
    std::map<std::string, Ranges> clients_;
    static std::pair<std::string, uint64_t> parse(const std::string& key) {
        auto split = key.rfind('/');
        if (split == std::string::npos) throw std::invalid_argument("invalid request key");
        uint64_t id = 0;
        const auto result = std::from_chars(key.data() + split + 1, key.data() + key.size(), id);
        if (result.ec != std::errc{} || result.ptr != key.data() + key.size())
            throw std::invalid_argument("invalid request ID");
        return {key.substr(0, split), id};
    }
public:
    // The request path holds the client ID and request ID separately. Taking
    // them directly keeps a joined key, and the split that undoes it, off the
    // hot path; the string overloads serve recovery and the legacy protocols.
    size_t count(const std::string& client, uint64_t id) const {
        auto found = clients_.find(client);
        if (found == clients_.end()) return 0;
        auto next = found->second.upper_bound(id);
        return next != found->second.begin() && std::prev(next)->second >= id;
    }
    size_t count(const std::string& key) const {
        const auto [client, id] = parse(key);
        return count(client, id);
    }
    std::pair<int, bool> insert(const std::string& key) {
        const auto [client, id] = parse(key);
        return insert(client, id);
    }
    std::pair<int, bool> insert(const std::string& client, uint64_t id) {
        auto& ranges = clients_[client];
        auto next = ranges.upper_bound(id);
        auto start = id, end = id;
        if (next != ranges.begin()) {
            auto previous = std::prev(next);
            if (previous->second >= id) return {0, false};
            if (previous->second == id - 1 && id != 0) {
                start = previous->first;
                ranges.erase(previous);
            }
        }
        if (next != ranges.end() && id != std::numeric_limits<uint64_t>::max() && next->first == id + 1) {
            end = next->second; ranges.erase(next);
        }
        ranges.emplace(start, end);
        return {0, true};
    }
    void erase(const std::string& key) {
        const auto [client, id] = parse(key);
        auto found = clients_.find(client);
        if (found == clients_.end()) return;
        auto next = found->second.upper_bound(id);
        if (next == found->second.begin()) return;
        auto range = std::prev(next);
        const auto first = range->first, last = range->second;
        if (last < id) return;
        found->second.erase(range);
        if (first < id) found->second.emplace(first, id - 1);
        if (id < last) found->second.emplace(id + 1, last);
        if (found->second.empty()) clients_.erase(found);
    }
    uint64_t size() const {
        uint64_t total = 0;
        for (const auto& [client, ranges] : clients_)
            for (const auto& [first, last] : ranges) total += last - first + 1;
        return total;
    }
    nlohmann::json snapshot() const {
        auto result = nlohmann::json::object();
        for (const auto& [client, ranges] : clients_) {
            auto encoded = nlohmann::json::array();
            for (auto [first, last] : ranges) encoded.push_back({first, last});
            result[client] = std::move(encoded);
        }
        return result;
    }
    static ExecutedRequests restore(const nlohmann::json& state) {
        if (!state.is_object()) throw std::invalid_argument("invalid replay snapshot");
        ExecutedRequests result;
        for (auto it = state.begin(); it != state.end(); ++it) {
            if (it.key().empty() || !it.value().is_array() || it.value().empty())
                throw std::invalid_argument("invalid replay client ranges");
            uint64_t previous = 0;
            bool firstRange = true;
            for (const auto& range : it.value()) {
                if (!range.is_array() || range.size() != 2 || !range[0].is_number_unsigned() || !range[1].is_number_unsigned())
                    throw std::invalid_argument("invalid replay range");
                const auto first = range[0].get<uint64_t>(), last = range[1].get<uint64_t>();
                if (first > last || (!firstRange && (previous == std::numeric_limits<uint64_t>::max() || first <= previous + 1)))
                    throw std::invalid_argument("noncanonical replay ranges");
                result.clients_[it.key()].emplace(first, last);
                previous = last; firstRange = false;
            }
        }
        return result;
    }
};
} // namespace bedrock
