#include "coordination/grpc/NodeServiceImpl.h"
#include "core/Entity.h"
#include <nlohmann/json.hpp>
#include <thread>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <thread>
#include <algorithm>
#include <string>
#include <vector>

using json = nlohmann::json;

namespace {
// Simple global worker pool for all RPC handlers
class ThreadPool {
public:
    void start(size_t n) {
        std::lock_guard<std::mutex> lk(m_);
        if (!threads_.empty()) return;
        stop_ = false;
        for (size_t i = 0; i < n; ++i) {
            threads_.emplace_back([this]() {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(m_);
                        cv_.wait(lk, [this]{ return stop_ || !q_.empty(); });
                        if (stop_ && q_.empty()) return;
                        task = std::move(q_.front());
                        q_.pop();
                    }
                    task();
                }
            });
        }
    }
    void post(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lk(m_);
            q_.push(std::move(fn));
        }
        cv_.notify_one();
    }
    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(m_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : threads_) if (t.joinable()) t.join();
        threads_.clear();
    }
private:
    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool stop_{false};
};

ThreadPool& rpcPool() {
    static ThreadPool pool;
    static std::once_flag once;
    std::call_once(once, []{
        const unsigned hw = std::max(2u, std::thread::hardware_concurrency());
        // Leave some headroom; gRPC sync server already runs multiple threads internally
        pool.start(std::min<unsigned>(16, std::max(2u, hw)));
    });
    return pool;
}
} // namespace

NodeServiceImpl::NodeServiceImpl(Entity& entity) : entity_(entity) {}

grpc::Status NodeServiceImpl::SubmitRequest(grpc::ServerContext*,
                                            const bedrock::ClientRequest* req,
                                            bedrock::Ack* ack) {
    json j = {
        {"type", "Request"},
        {"message_sender_id", req->message_sender_id()},
        {"timestamp", req->timestamp()},
        {"transaction", {
            {"from", req->transaction().from()},
            {"to", req->transaction().to()},
            {"amount", req->transaction().amount()}
        }},
        {"view", req->view()},
        {"operation", req->operation()},
        {"client_listen_port", req->client_listen_port()},
        {"signature", req->signature()}
    };
    // Queue work; respond immediately
    std::string payload = j.dump();
    rpcPool().post([this, payload]{
        (void)entity_.processJsonFromGrpc(payload);
    });

    ack->set_ok(true);
    ack->set_msg("accepted");
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::SendRawJson(grpc::ServerContext*,
                                          const bedrock::RawJson* request,
                                          bedrock::RawJson* response) {
    // Fire-and-forget on the pool
    std::string payload = request->json();
    rpcPool().post([this, payload]{
        (void)entity_.processJsonFromGrpc(payload);
    });

    response->set_json(R"({"status":"accepted"})");
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::SendProtocol(grpc::ServerContext*,
                                           const bedrock::ProtocolEnvelope* env,
                                           bedrock::Ack* ack) {
    // Debug log
    std::string kind = env->has_pre_prepare() ? "PrePrepare" :
                       env->has_prepare()     ? "Prepare" :
                       env->has_commit()      ? "Commit" : "Unknown";
    // std::cout << "[Node " << entity_.getNodeId() << "] Received " << kind << " via gRPC\n";

    // Copy the request so it's safe after RPC returns, then queue to pool
    bedrock::ProtocolEnvelope copy = *env;
    rpcPool().post([this, copy = std::move(copy)]() mutable {
        entity_.processProtocolEnvelope(copy);
    });

    ack->set_ok(true);
    ack->set_msg("accepted");
    return grpc::Status::OK;
}