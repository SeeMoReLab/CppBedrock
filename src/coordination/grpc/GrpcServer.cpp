#include "coordination/grpc/GrpcServer.h"
#include <grpcpp/grpcpp.h>
#include "proto/bedrock.grpc.pb.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <thread>
#include <atomic>
#include <memory>
#include <string>

namespace {

class NodeServiceImpl final : public bedrock::Node::Service {
public:
    explicit NodeServiceImpl(int backendTcpPort) : backendTcpPort_(backendTcpPort) {}

    grpc::Status SendRawJson(grpc::ServerContext*,
                             const bedrock::RawJson* request,
                             bedrock::RawJson* response) override {
        const std::string& json = request->json();
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "[gRPC Proxy] socket() failed\n";
            response->set_json(R"({"status":"socket_error"})");
            return grpc::Status::OK;
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(backendTcpPort_);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");

        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "[gRPC Proxy] connect() to backend " << backendTcpPort_ << " failed\n";
            close(sock);
            response->set_json(R"({"status":"connect_error"})");
            return grpc::Status::OK;
        }

        ssize_t sent = send(sock, json.data(), json.size(), 0);
        if (sent < 0) {
            std::cerr << "[gRPC Proxy] send() failed\n";
            close(sock);
            response->set_json(R"({"status":"send_error"})");
            return grpc::Status::OK;
        }

        // Optional: try a short read to echo any immediate reply (non-blocking behavior)
        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000; // 100ms
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        char buf[4096];
        int n = recv(sock, buf, sizeof(buf), 0);
        if (n > 0) {
            response->set_json(std::string(buf, n));
        } else {
            response->set_json(R"({"status":"ok"})");
        }
        close(sock);
        return grpc::Status::OK;
    }

private:
    int backendTcpPort_;
};

std::unique_ptr<grpc::Server> g_server;
std::unique_ptr<NodeServiceImpl> g_service;
std::thread g_serverThread;
std::atomic<bool> g_running{false};

} // namespace

namespace bedrockgrpc {

bool StartGrpcServer(int grpcPort, int backendTcpPort) {
    if (g_running.load()) return true;

    g_service = std::make_unique<NodeServiceImpl>(backendTcpPort);
    grpc::ServerBuilder builder;
    std::string addr = "0.0.0.0:" + std::to_string(grpcPort);
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(g_service.get());
    g_server = builder.BuildAndStart();
    if (!g_server) {
        std::cerr << "[gRPC Proxy] Failed to start on " << addr << "\n";
        g_service.reset();
        return false;
    }

    g_running.store(true);
    g_serverThread = std::thread([] {
        g_server->Wait();
        g_running.store(false);
    });
    std::cout << "[gRPC Proxy] Listening on " << addr << " (forwarding to TCP backend)\n";
    return true;
}

void StopGrpcServer() {
    if (!g_running.load()) return;
    g_server->Shutdown();
    if (g_serverThread.joinable()) g_serverThread.join();
    g_server.reset();
    g_service.reset();
    g_running.store(false);
}

} // namespace bedrockgrpc