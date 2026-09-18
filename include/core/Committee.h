#pragma once

// The replica committee: global replica ids 0..n-1 and the gRPC endpoint of
// each replica, loaded from the committee.json the harness generates
// (scripts/cloudlab/gen_committee.py --workspace bedrock):
//
//   {"replicas": [{"id": 0, "host": "10.252.1.10", "port": 15000}, ...]}
//
// Ids must be exactly 0..n-1: they double as the failure-spec and
// learning-agent replica ids, and the leader of view v is id (v mod n).

#include <string>
#include <vector>

namespace bedrock {

struct ReplicaEndpoint {
    int id{-1};
    std::string host;
    int port{0};

    std::string address() const { return host + ":" + std::to_string(port); }
};

class Committee {
public:
    // Parses and validates the committee file; throws std::runtime_error on
    // any malformed content.
    static Committee loadFromFile(const std::string& path);
    static Committee fromReplicas(std::vector<ReplicaEndpoint> replicas);

    int size() const { return static_cast<int>(replicas_.size()); }
    // Maximum tolerated faults for this committee size.
    int f() const { return (size() - 1) / 3; }
    const std::vector<ReplicaEndpoint>& replicas() const { return replicas_; }
    const ReplicaEndpoint& replica(int id) const;
    bool contains(int id) const { return id >= 0 && id < size(); }
    // Leader of a view under round-robin rotation over ids 0..n-1.
    int leaderForView(int view) const;

private:
    std::vector<ReplicaEndpoint> replicas_;  // sorted by id, ids 0..n-1
};

}  // namespace bedrock
