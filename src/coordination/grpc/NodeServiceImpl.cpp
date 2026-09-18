#include "coordination/grpc/NodeServiceImpl.h"
#include "core/Entity.h"
#include "core/Log.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

using json = nlohmann::json;

NodeServiceImpl::NodeServiceImpl(Entity& entity) : entity_(entity) {}

grpc::Status NodeServiceImpl::SendRawJson(grpc::ServerContext*,
                                          const bedrock::RawJson* request,
                                          bedrock::RawJson* response) {
    std::string payload = request->json();
    if (!entity_.processJsonFromGrpc(payload))
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid control message");
    response->set_json(R"({"status":"accepted"})");
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::SendProtocol(grpc::ServerContext*,
                                           const bedrock::ProtocolEnvelope* env,
                                           bedrock::Ack* ack) {
    entity_.processProtocolEnvelope(*env);
    ack->set_ok(true);
    ack->set_msg("accepted");
    return grpc::Status::OK;
}

grpc::Status NodeServiceImpl::ClientStream(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<bedrock::ClientReply, bedrock::ClientRequest>* stream) {
    std::string clientId;
    bedrock::ClientRequest req;
    while (stream->Read(&req)) {
        if (clientId.empty()) {
            if (req.client_id().empty()) {
                LOG_WARN("client stream from " << context->peer() << " sent a request without a client id; closing");
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "client_id required");
            }
            clientId = req.client_id();
            entity_.clientStreams().attach(clientId, stream, context);
            LOG_INFO("client stream attached: client=" << clientId << " peer=" << context->peer());
        } else if (req.client_id() != clientId) {
            LOG_WARN("client stream " << clientId << " carried a request for client " << req.client_id() << "; ignoring");
            continue;
        }
        if (req.request_id() == 0 && req.operation() == "hello") {
            continue;  // registration only
        }
        if (entity_.usesPbftCore()) {
            entity_.submitClientRequest(req);
            continue;
        }
        json j = {
            {"type", "Request"},
            {"client_id", req.client_id()},
            {"request_id", req.request_id()},
            {"timestamp", req.timestamp()},
            {"transaction", {
                {"from", req.transaction().from()},
                {"to", req.transaction().to()},
                {"amount", req.transaction().amount()}
            }},
            {"operation", req.operation()},
            {"signature", req.signature()},
            {"arrival_us", Entity::nowUs()}
        };
        std::string payload = j.dump();
        (void)entity_.processJsonFromGrpc(payload);
    }
    if (!clientId.empty()) {
        entity_.clientStreams().detach(clientId, stream);
        LOG_INFO("client stream detached: client=" << clientId);
    }
    return grpc::Status::OK;
}
