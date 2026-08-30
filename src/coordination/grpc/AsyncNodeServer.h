#pragma once
#include "bedrock.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include "core/Entity.h"
#include <functional>
#include <string>

class AsyncNodeServer {
public:
    explicit AsyncNodeServer(Entity& entity) : entity_(entity) {}
    ~AsyncNodeServer() { Shutdown(); }

    void Run(const std::string& addr, int pollerThreads = 4, int workerThreads = 4);
    void Shutdown();

private:
    struct ThreadPool {
        std::vector<std::thread> threads;
        std::queue<std::function<void()>> q;
        std::mutex m;
        std::condition_variable cv;
        bool stop{false};

        void start(size_t n) {
            for (size_t i = 0; i < n; ++i) {
                threads.emplace_back([this]{
                    for (;;) {
                        std::function<void()> task;
                        {
                            std::unique_lock<std::mutex> lk(m);
                            cv.wait(lk, [this]{ return stop || !q.empty(); });
                            if (stop && q.empty()) return;
                            task = std::move(q.front()); q.pop();
                        }
                        task();
                    }
                });
            }
        }
        void post(std::function<void()> fn) {
            {
                std::lock_guard<std::mutex> lk(m);
                q.push(std::move(fn));
            }
            cv.notify_one();
        }
        void join() {
            {
                std::lock_guard<std::mutex> lk(m);
                stop = true;
            }
            cv.notify_all();
            for (auto& t : threads) if (t.joinable()) t.join();
        }
    };

    // CallData base
    struct CallDataBase {
        virtual void Proceed(bool ok) = 0;
        virtual ~CallDataBase() = default;
    };

    template<typename Req, typename Resp>
    struct CallDataUnary : CallDataBase {
        bedrock::Node::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        AsyncNodeServer* server;
        grpc::ServerContext ctx;
        Req request;
        Resp reply;
        grpc::ServerAsyncResponseWriter<Resp> responder;
        enum State { CREATE, PROCESS, FINISH } state{CREATE};

        using RequestFn = void (bedrock::Node::AsyncService::*)(
            grpc::ServerContext*, Req*, grpc::ServerAsyncResponseWriter<Resp>*,
            grpc::ServerCompletionQueue*, grpc::ServerCompletionQueue*, void*);

        RequestFn requestFn;

        CallDataUnary(bedrock::Node::AsyncService* s,
                      grpc::ServerCompletionQueue* cq_,
                      AsyncNodeServer* srv,
                      RequestFn fn)
            : service(s), cq(cq_), server(srv), responder(&ctx), requestFn(fn) { Proceed(true); }

        void Proceed(bool ok) override {
            if (state == CREATE) {
                state = PROCESS;
                (service->*requestFn)(&ctx, &request, &responder, cq, cq, this);
            } else if (state == PROCESS) {
                // Spawn a new instance to serve the next request.
                new CallDataUnary(service, cq, server, requestFn);

                // Dispatch work to worker pool; respond when done
                server->workers_.post([this]{
                    server->HandleRequest(request, reply);
                    state = FINISH;
                    responder.Finish(reply, grpc::Status::OK, this);
                });
            } else {
                delete this;
            }
        }
    };

    // Handlers for each RPC to fill replies
    void HandleRequest(const bedrock::ClientRequest& req, bedrock::Ack& ack);
    void HandleRequest(const bedrock::RawJson& req, bedrock::RawJson& resp);
    void HandleRequest(const bedrock::ProtocolEnvelope& req, bedrock::Ack& ack);

    // Members
    Entity& entity_;
    bedrock::Node::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    std::vector<std::thread> pollers_;
    ThreadPool workers_;
};