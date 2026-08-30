#include <grpcpp/grpcpp.h>
#include "bedrock.grpc.pb.h"
#include "NodeServiceImpl.h"
#include <iostream>
#include <thread>
#include <string>

int main(int argc, char** argv) {
    int nodeId = (argc > 1) ? std::stoi(argv[1]) : 0;
    std::string addr = "0.0.0.0:" + std::to_string(6000 + nodeId);
    NodeServiceImpl service(nodeId);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    std::cout << "gRPC node server listening on " << addr << std::endl;
    server->Wait();
    return 0;
}