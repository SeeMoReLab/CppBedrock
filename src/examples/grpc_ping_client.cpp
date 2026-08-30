#include <grpcpp/grpcpp.h>
#include "bedrock.grpc.pb.h"
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    int targetNode = (argc > 1) ? std::stoi(argv[1]) : 1;
    std::string addr = "localhost:" + std::to_string(6000 + targetNode);
    auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = bedrock::NodeService::NewStub(channel);

    bedrock::Ping ping;
    ping.set_logical_time(123);        // Set a test logical time (or from args)

    grpc::ClientContext ctx;
    bedrock::Pong pong;
    auto status = stub->PingPong(&ctx, ping, &pong);
    if (!status.ok()) {
        std::cerr << "PingPong RPC failed: " << status.error_message() << "\n";
        return 1;
    }
    std::cout << "Pong from node " << pong.node_id()
              << " logical_time=" << pong.logical_time() << "\n";
    return 0;
}