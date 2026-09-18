#pragma once

// Fire-and-forget replica-to-replica sender on the gRPC asynchronous API.
//
// One completion queue and one drain thread serve every peer, so sending a
// message costs a heap-allocated call object instead of a thread, and calls
// to a slow peer pipeline instead of waiting for each acknowledgement.
// Failures are counted and reported at a bounded rate; protocols tolerate
// loss through their own timeouts.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <grpcpp/grpcpp.h>
#include "proto/bedrock.grpc.pb.h"
#include "proto/bedrock.pb.h"

namespace bedrock {

class AsyncSender {
public:
    explicit AsyncSender(int selfId);
    ~AsyncSender();

    AsyncSender(const AsyncSender&) = delete;
    AsyncSender& operator=(const AsyncSender&) = delete;

    // Registers a peer endpoint ("host:port"). Must be called before send.
    void addPeer(int peerId, const std::string& address);
    void start();
    void shutdown();

    void sendProtocol(int peerId, const ProtocolEnvelope& env);
    void sendRawJson(int peerId, const std::string& json);

    // Snapshot counters for the periodic stats line.
    uint64_t sent() const { return sent_.load(); }
    uint64_t failed() const { return failed_.load(); }
    uint64_t inflight() const { return inflight_.load(); }

private:
    struct PendingCall;
    struct ProtocolCall;
    struct RawJsonCall;

    Node::Stub* stubFor(int peerId);
    void drain();
    void onCompleted(PendingCall* call, bool ok);

    int selfId_;
    grpc::CompletionQueue cq_;
    std::thread drainThread_;
    std::mutex stubsMtx_;
    std::unordered_map<int, std::unique_ptr<Node::Stub>> stubs_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> sent_{0};
    std::atomic<uint64_t> failed_{0};
    std::atomic<uint64_t> inflight_{0};
    std::atomic<uint64_t> loggedFailures_{0};
};

}  // namespace bedrock
