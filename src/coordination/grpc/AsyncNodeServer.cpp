#include "coordination/grpc/AsyncNodeServer.h"
#include <nlohmann/json.hpp>
#include <string>

using json = nlohmann::json;

void AsyncNodeServer::Run(const std::string& addr, int pollerThreads, int workerThreads) {
    grpc::ServerBuilder b;
    b.AddListeningPort(addr, grpc::InsecureServerCredentials());
    b.RegisterService(&service_);
    cq_ = b.AddCompletionQueue();
    server_ = b.BuildAndStart();

    workers_.start(workerThreads);

    // Prime CallData for each method
    new CallDataUnary<bedrock::ClientRequest, bedrock::Ack>(&service_, cq_.get(), this,
        &bedrock::Node::AsyncService::RequestSubmitRequest);
    new CallDataUnary<bedrock::RawJson, bedrock::RawJson>(&service_, cq_.get(), this,
        &bedrock::Node::AsyncService::RequestSendRawJson);
    new CallDataUnary<bedrock::ProtocolEnvelope, bedrock::Ack>(&service_, cq_.get(), this,
        &bedrock::Node::AsyncService::RequestSendProtocol);

    // CQ pollers
    for (int i = 0; i < pollerThreads; ++i) {
        pollers_.emplace_back([this]{
            void* tag; bool ok;
            while (cq_->Next(&tag, &ok)) {
                static_cast<CallDataBase*>(tag)->Proceed(ok);
            }
        });
    }
}

void AsyncNodeServer::Shutdown() {
    if (server_) {
        server_->Shutdown();
        if (cq_) cq_->Shutdown();
        for (auto& t : pollers_) if (t.joinable()) t.join();
        workers_.join();
        server_.reset();
        cq_.reset();
        pollers_.clear();
    }
}

void AsyncNodeServer::HandleRequest(const bedrock::ClientRequest& req, bedrock::Ack& ack) {
    // Same semantics as your current SubmitRequest, without extra threads
    json j = {
        {"type", "Request"},
        {"message_sender_id", req.message_sender_id()},
        {"timestamp", req.timestamp()},
        {"transaction", {
            {"from", req.transaction().from()},
            {"to", req.transaction().to()},
            {"amount", req.transaction().amount()}
        }},
        {"view", req.view()},
        {"operation", req.operation()},
        {"client_listen_port", req.client_listen_port()},
        {"signature", req.signature()}
    };
    entity_.processJsonFromGrpc(j.dump());
    ack.set_ok(true);
    ack.set_msg("accepted");
}

void AsyncNodeServer::HandleRequest(const bedrock::RawJson& req, bedrock::RawJson& resp) {
    entity_.processJsonFromGrpc(req.json());
    resp.set_json(R"({"status":"accepted"})");
}

void AsyncNodeServer::HandleRequest(const bedrock::ProtocolEnvelope& req, bedrock::Ack& ack) {
    entity_.processProtocolEnvelope(req); // typed fast-path; no extra thread
    ack.set_ok(true);
    ack.set_msg("accepted");
}