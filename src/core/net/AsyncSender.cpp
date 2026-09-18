#include "core/net/AsyncSender.h"

#include <chrono>
#include <stdexcept>

#include "core/Log.h"

namespace bedrock {

namespace {
// Long enough to ride out a slow peer under injected network delay, short
// enough that a dead peer's queued calls are reclaimed within a run.
constexpr int kCallDeadlineSeconds = 30;
}  // namespace

struct AsyncSender::PendingCall {
    virtual ~PendingCall() = default;
    grpc::ClientContext ctx;
    grpc::Status status;
    int peer{-1};
    const char* kind{"?"};
};

struct AsyncSender::ProtocolCall : PendingCall {
    Ack ack;
    std::unique_ptr<grpc::ClientAsyncResponseReader<Ack>> reader;
};

struct AsyncSender::RawJsonCall : PendingCall {
    RawJson response;
    std::unique_ptr<grpc::ClientAsyncResponseReader<RawJson>> reader;
};

AsyncSender::AsyncSender(int selfId) : selfId_(selfId) {}

AsyncSender::~AsyncSender() { shutdown(); }

void AsyncSender::addPeer(int peerId, const std::string& address) {
    std::lock_guard<std::mutex> lk(stubsMtx_);
    if (stubs_.count(peerId)) {
        throw std::runtime_error("peer " + std::to_string(peerId) + " registered twice");
    }
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    args.SetMaxSendMessageSize(64 * 1024 * 1024);
    // Keep the connection alive through idle stretches (e.g. view changes).
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    auto channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
    stubs_[peerId] = Node::NewStub(channel);
}

void AsyncSender::start() {
    if (running_.exchange(true)) return;
    drainThread_ = std::thread(&AsyncSender::drain, this);
}

void AsyncSender::shutdown() {
    if (!running_.exchange(false)) return;
    cq_.Shutdown();
    if (drainThread_.joinable()) drainThread_.join();
}

Node::Stub* AsyncSender::stubFor(int peerId) {
    std::lock_guard<std::mutex> lk(stubsMtx_);
    auto it = stubs_.find(peerId);
    if (it == stubs_.end()) {
        throw std::runtime_error("no endpoint registered for peer " + std::to_string(peerId));
    }
    return it->second.get();
}

void AsyncSender::sendProtocol(int peerId, const ProtocolEnvelope& env) {
    if (!running_.load()) return;
    auto* call = new ProtocolCall();
    call->peer = peerId;
    call->kind = "SendProtocol";
    call->ctx.set_wait_for_ready(true);
    call->ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(kCallDeadlineSeconds));
    call->reader = stubFor(peerId)->PrepareAsyncSendProtocol(&call->ctx, env, &cq_);
    inflight_.fetch_add(1);
    sent_.fetch_add(1);
    call->reader->StartCall();
    call->reader->Finish(&call->ack, &call->status, call);
}

void AsyncSender::sendRawJson(int peerId, const std::string& json) {
    if (!running_.load()) return;
    auto* call = new RawJsonCall();
    call->peer = peerId;
    call->kind = "SendRawJson";
    call->ctx.set_wait_for_ready(true);
    call->ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(kCallDeadlineSeconds));
    RawJson request;
    request.set_json(json);
    call->reader = stubFor(peerId)->PrepareAsyncSendRawJson(&call->ctx, request, &cq_);
    inflight_.fetch_add(1);
    sent_.fetch_add(1);
    call->reader->StartCall();
    call->reader->Finish(&call->response, &call->status, call);
}

void AsyncSender::drain() {
    void* tag = nullptr;
    bool ok = false;
    while (cq_.Next(&tag, &ok)) {
        onCompleted(static_cast<PendingCall*>(tag), ok);
    }
}

void AsyncSender::onCompleted(PendingCall* call, bool ok) {
    inflight_.fetch_sub(1);
    if (!ok || !call->status.ok()) {
        const uint64_t failures = failed_.fetch_add(1) + 1;
        // First failure, then every 1000th, to keep a dead peer from
        // flooding the log.
        if (failures == 1 || failures % 1000 == 0) {
            LOG_WARN("gRPC " << call->kind << " to replica " << call->peer << " failed (" << failures
                              << " failures so far): " << call->status.error_code() << " "
                              << call->status.error_message());
        }
    }
    delete call;
}

}  // namespace bedrock
