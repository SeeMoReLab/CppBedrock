#include "core/Committee.h"

#include <algorithm>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace bedrock {

Committee Committee::loadFromFile(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("cannot open committee file: " + path);
    }
    nlohmann::json j;
    try {
        in >> j;
    } catch (const std::exception& e) {
        throw std::runtime_error("committee file " + path + " is not valid JSON: " + e.what());
    }
    if (!j.is_object() || !j.contains("replicas") || !j["replicas"].is_array()) {
        throw std::runtime_error("committee file " + path + " must be an object with a 'replicas' array");
    }
    std::vector<ReplicaEndpoint> replicas;
    for (const auto& r : j["replicas"]) {
        if (!r.is_object() || !r.contains("id") || !r.contains("host") || !r.contains("port")) {
            throw std::runtime_error("committee file " + path + ": every replica needs id, host, and port");
        }
        ReplicaEndpoint ep;
        ep.id = r["id"].get<int>();
        ep.host = r["host"].get<std::string>();
        ep.port = r["port"].get<int>();
        replicas.push_back(std::move(ep));
    }
    try {
        return fromReplicas(std::move(replicas));
    } catch (const std::exception& e) {
        throw std::runtime_error("committee file " + path + ": " + e.what());
    }
}

Committee Committee::fromReplicas(std::vector<ReplicaEndpoint> replicas) {
    if (replicas.size() < 4) {
        throw std::runtime_error("a committee needs at least 4 replicas, got " + std::to_string(replicas.size()));
    }
    std::sort(replicas.begin(), replicas.end(),
              [](const ReplicaEndpoint& a, const ReplicaEndpoint& b) { return a.id < b.id; });
    for (size_t i = 0; i < replicas.size(); ++i) {
        const auto& ep = replicas[i];
        if (ep.id != static_cast<int>(i)) {
            throw std::runtime_error("replica ids must be exactly 0..n-1 (missing or duplicate id " +
                                     std::to_string(i) + ")");
        }
        if (ep.host.empty()) {
            throw std::runtime_error("replica " + std::to_string(ep.id) + " has an empty host");
        }
        if (ep.port <= 0 || ep.port > 65535) {
            throw std::runtime_error("replica " + std::to_string(ep.id) + " has an invalid port " +
                                     std::to_string(ep.port));
        }
    }
    Committee c;
    c.replicas_ = std::move(replicas);
    return c;
}

const ReplicaEndpoint& Committee::replica(int id) const {
    if (!contains(id)) {
        throw std::out_of_range("replica id " + std::to_string(id) + " is not in the committee");
    }
    return replicas_[static_cast<size_t>(id)];
}

int Committee::leaderForView(int view) const {
    if (view < 0) {
        throw std::invalid_argument("view must be non-negative, got " + std::to_string(view));
    }
    return replicas_[static_cast<size_t>(view % size())].id;
}

}  // namespace bedrock
