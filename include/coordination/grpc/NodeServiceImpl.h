#pragma once

#include <grpcpp/grpcpp.h>
#include "proto/bedrock.grpc.pb.h"

class Entity;

// gRPC service hosted by every replica: replica-to-replica messages are
// processed before acknowledgement. Client streams use bounded ingress
// separate from consensus processing, and separate writers for replies.
class NodeServiceImpl final : public bedrock::Node::Service {
public:
    explicit NodeServiceImpl(Entity& entity);

    grpc::Status SendRawJson(grpc::ServerContext*,
                             const bedrock::RawJson*,
                             bedrock::RawJson*) override;

    grpc::Status SendProtocol(grpc::ServerContext*,
                              const bedrock::ProtocolEnvelope*,
                              bedrock::Ack*) override;

    grpc::Status ClientStream(grpc::ServerContext*,
                              grpc::ServerReaderWriter<bedrock::ClientReply, bedrock::ClientRequest>*) override;

private:
    Entity& entity_;
};
