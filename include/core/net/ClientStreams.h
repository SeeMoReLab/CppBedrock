#pragma once

// Registry of client streams. Consensus queues replies without performing
// socket writes. Each stream owns a bounded queue and one writer thread;
// detach cancels and joins that writer before the RPC handler returns.
// Replies with identical metadata are coalesced for up to 1 ms.

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "proto/bedrock.grpc.pb.h"

namespace bedrock {

class ClientStreams {
public:
    using Writer = grpc::ServerReaderWriter<ClientReply, ClientRequest>;

    // Registers the stream for clientId; replaces an older stream of the
    // same client (a reconnect).
    void attach(const std::string& clientId, Writer* writer, grpc::ServerContext* context);
    // Marks the stream closed and drops it; waits for in-flight writes.
    void detach(const std::string& clientId, Writer* writer);
    // Writes a reply on the client's stream. Returns false when the client
    // has no open stream on this replica.
    bool reply(const std::string& clientId, const ClientReply& reply);
    // Queues a whole batch of replies for one client. An executed batch holds
    // thousands of requests, and replying one at a time costs a mutex pair and
    // a condition-variable wake each. Returns the number queued; the batch is
    // consumed either way.
    size_t reply(const std::string& clientId, std::vector<ClientReply>& replies);
    size_t size() const;
    uint64_t droppedReplies() const { return dropped_; }

private:
    struct Stream {
        Writer* writer{nullptr};
        grpc::ServerContext* context{nullptr};
        std::mutex writeMtx;
        std::condition_variable cv;
        std::deque<ClientReply> queue;
        std::thread worker;
        bool closed{false};
    };

    mutable std::mutex mtx_;
    std::unordered_map<std::string, std::shared_ptr<Stream>> streams_;
    std::unordered_map<Writer*, std::shared_ptr<Stream>> attached_;
    std::atomic<uint64_t> dropped_{0};
    static void close(const std::shared_ptr<Stream>& stream);
};

}  // namespace bedrock
