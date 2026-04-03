#include "core/crypto/OpenSSLCryptoProvider.h"
#include "coordination/connections/TcpConnection.h"
#include <nlohmann/json.hpp>
#include <thread>
#include <iostream>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <atomic>
#include <vector>
#include <mutex>
#include <queue>
#include <map>
#include <set>
#include <unordered_map>
#include <arpa/inet.h>
#include "utils/Benchmark.h" // NEW

// gRPC
#include <grpcpp/grpcpp.h>
#include "proto/bedrock.grpc.pb.h"
#include "proto/bedrock.pb.h"

using json = nlohmann::json;

namespace {
// Cache: port -> gRPC stub
static std::unordered_map<int, std::unique_ptr<bedrock::Node::Stub>> g_stubs;

static void initGrpcStubs(const std::vector<int>& ports) {
    for (int port : ports) {
        auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                           grpc::InsecureChannelCredentials());
        g_stubs.emplace(port, bedrock::Node::NewStub(channel));
    }
}

static bool grpcSendRawJson(int port, const std::string& payloadJson, std::string& outResponseJson) {
    auto it = g_stubs.find(port);
    if (it == g_stubs.end()) return false;

    bedrock::RawJson req;
    req.set_json(payloadJson);
    bedrock::RawJson resp;

    grpc::ClientContext ctx;
    auto status = it->second->SendRawJson(&ctx, req, &resp);
    if (!status.ok()) {
        std::cerr << "[gRPC] SendRawJson to " << port << " failed: "
                  << status.error_code() << " " << status.error_message() << "\n";
        return false;
    }
    outResponseJson = resp.json();
    return true;
}

static bool grpcSubmitRequest(int port, const json& j) {
    auto it = g_stubs.find(port);
    if (it == g_stubs.end()) return false;

    bedrock::ClientRequest req;
    req.set_message_sender_id(j.at("message_sender_id").get<std::string>());
    req.set_timestamp(j.at("timestamp").get<std::string>());

    const auto& txj = j.at("transaction");
    auto* tx = req.mutable_transaction();
    tx->set_from(txj.at("from").get<std::string>());
    tx->set_to(txj.at("to").get<std::string>());
    tx->set_amount(txj.at("amount").get<int>());

    req.set_view(j.at("view").get<int>());
    req.set_operation(j.at("operation").get<std::string>());
    req.set_client_listen_port(j.at("client_listen_port").get<int>());
    req.set_signature(j.at("signature").get<std::string>());

    bedrock::Ack ack;
    grpc::ClientContext ctx;
    auto status = it->second->SubmitRequest(&ctx, req, &ack);
    if (!status.ok()) {
        std::cerr << "[gRPC] SubmitRequest to " << port << " failed: "
                  << status.error_code() << " " << status.error_message() << "\n";
        return false;
    }
    return ack.ok();
}
} // namespace

struct Transaction {
    std::string id;      // timestamp
    std::string payload; // serialized json
    int retries = 0;
};

static Benchmark scenario4Bench("scenario4"); // NEW

int main(int argc, char* argv[]) {
    int leaderPort = 5001;
    int clientListenPort = 6000;
    int scenario = 1; // Default to 1=single

    // Parse scenario from command line argument if provided
    if (argc > 1) {
        scenario = std::stoi(argv[1]);
        if (scenario < 1) {
            std::cerr << "Invalid scenario. Use 1 (single), 2 (sequential), 3 (concurrent), 4 (randomized), 5 (failure test), 6 (HotStuff), 7 (Zyzzyva), or 8 (Client-triggered view change)." << std::endl;
            return 1;
        }
    }

    if (scenario == 4) {
        scenario4Bench.reset("scenario4"); // was: scenario4Bench = Benchmark("scenario4");
    }

    std::atomic<bool> running{true};
    std::vector<std::string> responses;
    std::mutex responsesMutex;

    std::queue<Transaction> txnQueue;
    std::map<std::string, int> txnResponses;
    std::set<std::string> completedTxns;
    std::mutex txnMutex;

    // Zyzzyva client-side tracking (NEW)
    std::unordered_map<std::string, std::set<int>> txnResponders; // txnId -> unique replica ids
    std::unordered_map<std::string, int> txnSeq;                  // txnId -> sequence

    std::vector<int> nodePorts = {5001, 5002, 5003, 5004};
    const int n = nodePorts.size();
    const int f = (n - 1) / 3;
    int requiredResponses = 2 * f + 1;
    const int maxRetries = 5;
    const int responseTimeoutSec = 2;
    int NUM_REQUESTS = 3;

    // Init gRPC only for scenario 4 (on ports +10000)
    if (true) {
        // For scenario 4, require n-1 unique replies per txn
        requiredResponses = n - f;
        std::vector<int> grpcPorts;
        grpcPorts.reserve(nodePorts.size());
        for (int p : nodePorts) grpcPorts.push_back(p + 10000);
        initGrpcStubs(grpcPorts);
    }

    std::map<std::string, int> initialBalances = {
        {"A", 100},
        {"B", 100},
        {"C", 100},
        {"D", 100}
    };
    std::vector<std::tuple<std::string, std::string, int>> transactions;

    // Start a server thread to listen for incoming responses
    std::thread serverThread([&]() {
        int listenSock = socket(AF_INET, SOCK_STREAM, 0);
        if (listenSock < 0) {
            std::cerr << "[Listener] Failed to create socket\n";
            return;
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(clientListenPort);
        addr.sin_addr.s_addr = INADDR_ANY;
        if (bind(listenSock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "[Listener] Bind failed\n";
            close(listenSock);
            return;
        }
        listen(listenSock, 10);
        std::cout << "[Listener] Listening on port " << clientListenPort << std::endl;

        while (running) {
            sockaddr_in clientAddr;
            socklen_t clientAddrLen = sizeof(clientAddr);
            int respSock = accept(listenSock, (struct sockaddr*)&clientAddr, &clientAddrLen);
            if (respSock < 0) {
                if (running) std::cerr << "[Listener] Accept failed\n";
                continue;
            }
            char buffer[4096] = {0};
            int bytesReceived = recv(respSock, buffer, sizeof(buffer), 0);
            if (bytesReceived > 0) {
                std::string response(buffer, bytesReceived);
                //std::cout << "[Listener] Received response: " << response << std::endl;
                {
                    std::lock_guard<std::mutex> lock(responsesMutex);
                    responses.push_back(response);
                }
                // Parse operation/timestamp from response and count
                try {
                    auto j = json::parse(response);
                    std::string op;
                    if (j.contains("operation") && j["operation"].is_string()) {
                        op = j["operation"].get<std::string>();
                    } else if (j.contains("timestamp") && j["timestamp"].is_string()) {
                        op = j["timestamp"].get<std::string>();
                    }
                    if (!op.empty()) {
                        // Count distinct responder ids and capture sequence (NEW)
                        int senderId = -1;
                        if (j.contains("message_sender_id")) {
                            if (j["message_sender_id"].is_number_integer()) {
                                senderId = j["message_sender_id"].get<int>();
                            } else if (j["message_sender_id"].is_string()) {
                                try { senderId = std::stoi(j["message_sender_id"].get<std::string>()); } catch (...) {}
                            }
                        }
                        int seq = -1;
                        if (j.contains("sequence") && j["sequence"].is_number_integer()) {
                            seq = j["sequence"].get<int>();
                        }
                        std::lock_guard<std::mutex> txnLock(txnMutex);
                        if (senderId != -1) {
                            txnResponders[op].insert(senderId);
                            txnResponses[op] = static_cast<int>(txnResponders[op].size());
                        } else {
                            // fallback if sender is missing
                            txnResponses[op]++;
                        }
                        if (seq != -1) txnSeq[op] = seq;
                        
                        if (txnResponses[op] >= requiredResponses) {
                            completedTxns.insert(op);
                        }
                        
                        if (scenario == 4) scenario4Bench.end(op); // NEW
                    }
                } catch (...) {}
                // std::cout << "[Listener] Received response: " << response << std::endl;
            }
            close(respSock);
        }
        close(listenSock);
    });

    OpenSSLCryptoProvider crypto("../keys/client_private.pem");
    auto startTime = std::chrono::high_resolution_clock::now();

    // === Transaction Preparation based on scenario ===
    if (scenario == 1) {
        // Example: A->B, B->C, C->D
        transactions = {
            {"A", "B", 30}
        };
        NUM_REQUESTS = transactions.size();
        int txnIdx = 0;
        int grpcLeaderPort = leaderPort + 10000; // e.g. 15001
    
        for (const auto& [from, to, amount] : transactions) {
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx++);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
    
            // Send via gRPC SubmitRequest to leader
            if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed for " << timestamp << ".\n";
            }
    
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    } else if (scenario == 2) {
        // Example: C->A, D->B, A->C, B->D, repeated
        NUM_REQUESTS = 10;
        transactions.clear();
        for (int i = 0; i < NUM_REQUESTS; ++i) {
            if (i % 4 == 0) transactions.push_back({"C", "A", 15});
            else if (i % 4 == 1) transactions.push_back({"D", "B", 25});
            else if (i % 4 == 2) transactions.push_back({"A", "C", 10});
            else transactions.push_back({"B", "D", 20});
        }
        int txnIdx = 0;
        int grpcLeaderPort = leaderPort + 10000; // e.g. 15001
        
        for (const auto& [from, to, amount] : transactions) {
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx++);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
    
            // Send via gRPC SubmitRequest to leader
            if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed for " << timestamp << ".\n";
            }
    
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } else if (scenario == 3) {
        NUM_REQUESTS = 10; // reduce burst size

        transactions.clear();
        for (int i = 0; i < NUM_REQUESTS; ++i) {
            std::string from = (i % 4 == 0) ? "A" : (i % 4 == 1) ? "B" : (i % 4 == 2) ? "C" : "D";
            std::string to = (i % 4 == 0) ? "B" : (i % 4 == 1) ? "C" : (i % 4 == 2) ? "D" : "A";
            int amount = 5 + (i % 5) * 5;
            transactions.push_back({from, to, amount});
        }
        std::vector<std::thread> clientThreads;
        const int maxConcurrent = 128; // cap concurrency
        std::mutex gateMtx;
        std::condition_variable gateCv;
        int inFlight = 0;

        int txnIdx = 0;
        for (const auto& txn : transactions) {
            // throttle thread creation
            {
                std::unique_lock<std::mutex> lk(gateMtx);
                gateCv.wait(lk, [&]{ return inFlight < maxConcurrent; });
                ++inFlight;
            }
            clientThreads.emplace_back([&, txnIdx, txn]() {
                auto [from, to, amount] = txn;
                json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
                auto now = std::chrono::system_clock::now();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx);
                scenario4Bench.start(timestamp);
                std::string clientId = "client";
                json j = {
                    {"type", "Request"},
                    {"message_sender_id", clientId},
                    {"timestamp", timestamp},
                    {"transaction", transaction},
                    {"view", 0},
                    {"operation", timestamp},
                    {"client_listen_port", clientListenPort}
                };
                std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
                OpenSSLCryptoProvider crypto("../keys/client_private.pem");
                std::string signature = crypto.sign(msgToSign);
                j["signature"] = signature;

                // SEND VIA gRPC SubmitRequest (protobuf) to leader's gRPC port
                int grpcLeaderPort = leaderPort + 10000; // 15001 for leader 5001
                if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                    std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed.\n";
                }

                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    txnResponses[timestamp] = 0;
                }
                // release slot
                {
                    std::lock_guard<std::mutex> lk(gateMtx);
                    --inFlight;
                }
                gateCv.notify_one();
            });
            txnIdx++;
            // small pacing to avoid instant stampede
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (auto& t : clientThreads) t.join();

        // Wait until NUM_REQUESTS - 1 operations are completed (e.g., 39 of 40)
        {
            const int targetDone = std::max(0, NUM_REQUESTS - 1);
            const int maxWaitSec = 60;
            auto start = std::chrono::steady_clock::now();
            while (true) {
                size_t done = 0;
                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    done = completedTxns.size();
                }
                if (done >= static_cast<size_t>(targetDone)) break;
                if (std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - start).count() > maxWaitSec) {
                    std::cout << "[Scenario 4] Timeout waiting: completed " << done
                              << "/" << targetDone << " ops.\n";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }
    else if (scenario == 4) {
        NUM_REQUESTS = 100; // reduce burst size

        transactions.clear();
        for (int i = 0; i < NUM_REQUESTS; ++i) {
            std::string from = (i % 4 == 0) ? "A" : (i % 4 == 1) ? "B" : (i % 4 == 2) ? "C" : "D";
            std::string to = (i % 4 == 0) ? "B" : (i % 4 == 1) ? "C" : (i % 4 == 2) ? "D" : "A";
            int amount = 5 + (i % 5) * 5;
            transactions.push_back({from, to, amount});
        }
        std::vector<std::thread> clientThreads;
        const int maxConcurrent = 128; // cap concurrency
        std::mutex gateMtx;
        std::condition_variable gateCv;
        int inFlight = 0;

        int txnIdx = 0;
        for (const auto& txn : transactions) {
            // throttle thread creation
            {
                std::unique_lock<std::mutex> lk(gateMtx);
                gateCv.wait(lk, [&]{ return inFlight < maxConcurrent; });
                ++inFlight;
            }
            clientThreads.emplace_back([&, txnIdx, txn]() {
                auto [from, to, amount] = txn;
                json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
                auto now = std::chrono::system_clock::now();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx);
                scenario4Bench.start(timestamp);
                std::string clientId = "client";
                json j = {
                    {"type", "Request"},
                    {"message_sender_id", clientId},
                    {"timestamp", timestamp},
                    {"transaction", transaction},
                    {"view", 0},
                    {"operation", timestamp},
                    {"client_listen_port", clientListenPort}
                };
                std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
                OpenSSLCryptoProvider crypto("../keys/client_private.pem");
                std::string signature = crypto.sign(msgToSign);
                j["signature"] = signature;

                // SEND VIA gRPC SubmitRequest (protobuf) to leader's gRPC port
                int grpcLeaderPort = leaderPort + 10000; // 15001 for leader 5001
                if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                    std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed.\n";
                }

                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    txnResponses[timestamp] = 0;
                }
                // release slot
                {
                    std::lock_guard<std::mutex> lk(gateMtx);
                    --inFlight;
                }
                gateCv.notify_one();
            });
            txnIdx++;
            // small pacing to avoid instant stampede
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (auto& t : clientThreads) t.join();

        // Wait until NUM_REQUESTS - 1 operations are completed (e.g., 39 of 40)
        {
            const int targetDone = std::max(0, NUM_REQUESTS - 1);
            const int maxWaitSec = 60;
            auto start = std::chrono::steady_clock::now();
            while (true) {
                size_t done = 0;
                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    done = completedTxns.size();
                }
                if (done >= static_cast<size_t>(targetDone)) break;
                if (std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - start).count() > maxWaitSec) {
                    std::cout << "[Scenario 4] Timeout waiting: completed " << done
                              << "/" << targetDone << " ops.\n";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }
    else if (scenario == 5) {
        // Example: A->B, B->C, C->D, D->A
        NUM_REQUESTS = 8000; // reduce burst size

        transactions.clear();
        for (int i = 0; i < NUM_REQUESTS; ++i) {
            std::string from = (i % 4 == 0) ? "A" : (i % 4 == 1) ? "B" : (i % 4 == 2) ? "C" : "D";
            std::string to = (i % 4 == 0) ? "B" : (i % 4 == 1) ? "C" : (i % 4 == 2) ? "D" : "A";
            int amount = 5 + (i % 5) * 5;
            transactions.push_back({from, to, amount});
        }
        std::vector<std::thread> clientThreads;
        const int maxConcurrent = 512; // cap concurrency
        std::mutex gateMtx;
        std::condition_variable gateCv;
        int inFlight = 0;

        int txnIdx = 0;
        for (const auto& txn : transactions) {
            // throttle thread creation
            {
                std::unique_lock<std::mutex> lk(gateMtx);
                gateCv.wait(lk, [&]{ return inFlight < maxConcurrent; });
                ++inFlight;
            }
            clientThreads.emplace_back([&, txnIdx, txn]() {
                auto [from, to, amount] = txn;
                json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
                auto now = std::chrono::system_clock::now();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx);
                scenario4Bench.start(timestamp);
                std::string clientId = "client";
                json j = {
                    {"type", "Request"},
                    {"message_sender_id", clientId},
                    {"timestamp", timestamp},
                    {"transaction", transaction},
                    {"view", 0},
                    {"operation", timestamp},
                    {"client_listen_port", clientListenPort}
                };
                std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
                OpenSSLCryptoProvider crypto("../keys/client_private.pem");
                std::string signature = crypto.sign(msgToSign);
                j["signature"] = signature;

                // SEND VIA gRPC SubmitRequest (protobuf) to leader's gRPC port
                int grpcLeaderPort = leaderPort + 10000; // 15001 for leader 5001
                if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                    std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed.\n";
                }

                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    txnResponses[timestamp] = 0;
                }
                // release slot
                {
                    std::lock_guard<std::mutex> lk(gateMtx);
                    --inFlight;
                }
                gateCv.notify_one();
            });
            txnIdx++;
            // small pacing to avoid instant stampede
            // std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (auto& t : clientThreads) t.join();

        // Wait until NUM_REQUESTS - 1 operations are completed (e.g., 39 of 40)
        {
            const int targetDone = std::max(0, NUM_REQUESTS - 1);
            const int maxWaitSec = 60;
            auto start = std::chrono::steady_clock::now();
            while (true) {
                size_t done = 0;
                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    done = completedTxns.size();
                }
                if (done >= static_cast<size_t>(targetDone)) break;
                if (std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - start).count() > maxWaitSec) {
                    std::cout << "[Scenario 4] Timeout waiting: completed " << done
                              << "/" << targetDone << " ops.\n";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }
    else if (scenario == 6) {
        // Example: A->B
        transactions = {
            {"A", "B", 30}
        };
        NUM_REQUESTS = transactions.size();
        int txnIdx = 0;
    
        for (const auto& [from, to, amount] : transactions) {
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx++);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
    
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
    
            // enqueue for generic retry loop
            Transaction t;
            t.id = timestamp;
            t.payload = j.dump();
            t.retries = 0;
            txnQueue.push(t);
        }
    }
    else if (scenario == 7) {
        // Example: A->B
        transactions = {
            {"A", "B", 30}
        };
        NUM_REQUESTS = transactions.size();
        int txnIdx = 0;
    
        for (const auto& [from, to, amount] : transactions) {
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx++);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
    
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
    
            // enqueue for generic retry loop
            Transaction t;
            t.id = timestamp;
            t.payload = j.dump();
            t.retries = 0;
            txnQueue.push(t);
        }
    }
    else if (scenario == 8) {
        // Example: A->B
        transactions = {
            {"A", "B", 30}
        };
        NUM_REQUESTS = transactions.size();
        int txnIdx = 0;
    
        for (const auto& [from, to, amount] : transactions) {
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx++);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
    
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
    
            // enqueue for generic retry loop
            Transaction t;
            t.id = timestamp;
            t.payload = j.dump();
            t.retries = 0;
            txnQueue.push(t);
        }
    }
    else if (scenario == 9) {
        // Zyzzyva scenario: send only to the leader, wait for 2f+1 speculative responses, send commit if needed
        transactions = {
            {"A", "B", 50},
            {"B", "C", 30},
            {"C", "D", 20}
        };
        // Zyzzyva scenario: send to all nodes, wait for 2f+1 speculative responses, send commit if needed
        
        NUM_REQUESTS = transactions.size();
        int txnIdx = 0;
        for (const auto& [from, to, amount] : transactions) {
            int leaderIdx = 0; // round robin leader
            int leaderPortForTxn = nodePorts[leaderIdx];
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
            std::string strtoSend = j.dump();

            // Send to all nodes to simulate Zyzzyva speculative execution
            try {
                TcpConnection clientConn(leaderPortForTxn, false);
                clientConn.send(strtoSend);
                clientConn.closeConnection();
                std::cout << "[HotStuffTest] Sent txn " << txnIdx << " to leader on port " << leaderPortForTxn << "\n";
            } catch (...) {
                std::cout << "[HotStuffTest] Could not connect to leader on port " << leaderPortForTxn << ".\n";
            }
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
            txnIdx++;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        // Wait for speculative responses and send commit if needed
        for (const auto& [from, to, amount] : transactions) {
            std::string txnId;
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnId = txnResponses.begin()->first;
            }
            bool enoughResponses = false;
            auto waitStart = std::chrono::steady_clock::now();
            while (true) {
                int respCount = 0;
                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    respCount = txnResponses[txnId];
                }
                if (respCount >= requiredResponses) {
                    std::cout << "[ZyzzyvaTest] Got " << respCount << " speculative responses for txn " << txnId << "\n";
                    enoughResponses = true;
                    completedTxns.insert(txnId);

                    // If between 2f+1 and 3f+1, send commit to all nodes
                    if (respCount >= requiredResponses && respCount < n) {
                        int seqForCommit = -1;
                        {
                            std::lock_guard<std::mutex> lock(txnMutex);
                            auto it = txnSeq.find(txnId);
                            if (it != txnSeq.end()) seqForCommit = it->second;
                        }
                        json commitMsg = {
                            {"type", "Commit"},
                            {"timestamp", txnId},
                            {"operation", txnId},
                            {"view", 0},
                            {"message_sender_id", -1},
                            {"client_listen_port", clientListenPort}
                        };
                        if (seqForCommit != -1) {
                            commitMsg["sequence"] = seqForCommit; // include explicit sequence (NEW)
                        }
                        std::string commitStr = commitMsg.dump();
                        for (int port : nodePorts) {
                            try {
                                TcpConnection nodeConn(port, false);
                                nodeConn.send(commitStr);
                                nodeConn.closeConnection();
                                std::cout << "[ZyzzyvaTest] Sent COMMIT for txn " << txnId << " to node on port " << port << "\n";
                            } catch (...) {
                                std::cout << "[ZyzzyvaTest] Could not connect to node " << port << " for commit.\n";
                            }
                        }
                    } else if (respCount == n) {
                        std::cout << "[ZyzzyvaTest] Got 3f+1 responses for txn " << txnId << ", no commit needed.\n";
                    }
                    break;
                }
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::seconds>(now - waitStart).count() > responseTimeoutSec) {
                    std::cout << "[ZyzzyvaTest] Timeout waiting for txn " << txnId << " responses. Triggering view change.\n";
                    // Client-triggered view change (NEW): ask replicas to initiate VC
                    json trig = {
                        {"type", "TriggerViewChange"},
                        {"reason", "ClientTimeout"},
                        {"message_sender_id", -1}
                    };
                    std::string trigStr = trig.dump();
                    for (int port : nodePorts) {
                        try {
                            TcpConnection nodeConn(port, false);
                            nodeConn.send(trigStr);
                            nodeConn.closeConnection();
                        } catch (...) {
                            std::cout << "[ZyzzyvaTest] Could not connect to node " << port << " to trigger view change.\n";
                        }
                    }
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }
    else if (scenario == 10) {
        // Scenario 8: Client-triggered view change
        transactions = {
            {"A", "B", 50},
            {"B", "C", 30},
            {"C", "D", 20}
        };
        NUM_REQUESTS = transactions.size();
        int txnIdx = 0;

        // Step 1: Send transactions to the leader
        for (const auto& [from, to, amount] : transactions) {
            int leaderIdx = 0; // Round-robin leader selection
            int leaderPortForTxn = nodePorts[leaderIdx];
            json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
            auto now = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx);
            std::string clientId = "client";
            json j = {
                {"type", "Request"},
                {"message_sender_id", clientId},
                {"timestamp", timestamp},
                {"transaction", transaction},
                {"view", 0},
                {"operation", timestamp},
                {"client_listen_port", clientListenPort}
            };
            std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
            std::string signature = crypto.sign(msgToSign);
            j["signature"] = signature;
            std::string strtoSend = j.dump();

            // Send transaction to the leader
            try {
                TcpConnection clientConn(leaderPortForTxn, false);
                clientConn.send(strtoSend);
                clientConn.closeConnection();
                std::cout << "[Scenario 8] Sent txn " << txnIdx << " to leader on port " << leaderPortForTxn << "\n";
            } catch (...) {
                std::cout << "[Scenario 8] Could not connect to leader on port " << leaderPortForTxn << ".\n";
            }
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                txnResponses[timestamp] = 0;
            }
            txnIdx++;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        // Step 2: Trigger view change from the client
        std::cout << "[Scenario 8] Triggering view change from the client.\n";
        json triggerViewChangeMsg = {
            {"type", "TriggerViewChange"},
            {"reason", "ClientTestScenario"},
            {"message_sender_id", -1} // Client ID
        };
        std::string triggerViewChangeStr = triggerViewChangeMsg.dump();

        for (int port : nodePorts) {
            try {
                TcpConnection nodeConn(port, false);
                nodeConn.send(triggerViewChangeStr);
                nodeConn.closeConnection();
                std::cout << "[Scenario 8] Sent TriggerViewChange to node on port " << port << "\n";
            } catch (...) {
                std::cout << "[Scenario 8] Could not connect to node " << port << " to trigger view change.\n";
            }
        }

        // Step 3: Wait for the view change to complete
        std::this_thread::sleep_for(std::chrono::seconds(5)); // Adjust as needed

        // Step 4: Verify that the new leader is active
        std::cout << "[Scenario 8] Verifying that the new leader is active.\n";
        json query = {
            {"type", "QueryBalances"},
            {"message_sender_id", "client"},
            {"timestamp", std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count())},
            {"client_listen_port", clientListenPort}
        };
        std::string queryStr = query.dump();

        for (int port : nodePorts) {
            try {
                TcpConnection nodeConn(port, false);
                nodeConn.send(queryStr);
                nodeConn.closeConnection();
                std::cout << "[Scenario 8] Sent balance query to node on port " << port << "\n";
            } catch (...) {
                std::cout << "[Scenario 8] Could not connect to node " << port << ".\n";
            }
        }
    }
    else if (scenario == 11){
        NUM_REQUESTS = 1000; // reduce burst size

        transactions.clear();
        for (int i = 0; i < NUM_REQUESTS; ++i) {
            std::string from = (i % 4 == 0) ? "A" : (i % 4 == 1) ? "B" : (i % 4 == 2) ? "C" : "D";
            std::string to = (i % 4 == 0) ? "B" : (i % 4 == 1) ? "C" : (i % 4 == 2) ? "D" : "A";
            int amount = 5 + (i % 5) * 5;
            transactions.push_back({from, to, amount});
        }
        std::vector<std::thread> clientThreads;
        const int maxConcurrent = 128; // cap concurrency
        std::mutex gateMtx;
        std::condition_variable gateCv;
        int inFlight = 0;

        int txnIdx = 0;
        for (const auto& txn : transactions) {
            // throttle thread creation
            {
                std::unique_lock<std::mutex> lk(gateMtx);
                gateCv.wait(lk, [&]{ return inFlight < maxConcurrent; });
                ++inFlight;
            }
            clientThreads.emplace_back([&, txnIdx, txn]() {
                auto [from, to, amount] = txn;
                json transaction = {{"from", from}, {"to", to}, {"amount", amount}};
                auto now = std::chrono::system_clock::now();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                std::string timestamp = std::to_string(ms) + "_" + std::to_string(txnIdx);
                scenario4Bench.start(timestamp);
                std::string clientId = "client";
                json j = {
                    {"type", "Request"},
                    {"message_sender_id", clientId},
                    {"timestamp", timestamp},
                    {"transaction", transaction},
                    {"view", 0},
                    {"operation", timestamp},
                    {"client_listen_port", clientListenPort}
                };
                std::string msgToSign = j["transaction"].dump() + j["timestamp"].get<std::string>();
                OpenSSLCryptoProvider crypto("../keys/client_private.pem");
                std::string signature = crypto.sign(msgToSign);
                j["signature"] = signature;

                // SEND VIA gRPC SubmitRequest (protobuf) to leader's gRPC port
                int grpcLeaderPort = leaderPort + 10000; // 15001 for leader 5001
                if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                    std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed.\n";
                }

                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    txnResponses[timestamp] = 0;
                }
                // release slot
                {
                    std::lock_guard<std::mutex> lk(gateMtx);
                    --inFlight;
                }
                gateCv.notify_one();
            });
            txnIdx++;
            // small pacing to avoid instant stampede
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (auto& t : clientThreads) t.join();

        // Wait until NUM_REQUESTS - 1 operations are completed (e.g., 39 of 40)
        {
            const int targetDone = std::max(0, NUM_REQUESTS - 1);
            const int maxWaitSec = 60;
            auto start = std::chrono::steady_clock::now();
            while (true) {
                size_t done = 0;
                {
                    std::lock_guard<std::mutex> lock(txnMutex);
                    done = completedTxns.size();
                }
                if (done >= static_cast<size_t>(targetDone)) break;
                if (std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - start).count() > maxWaitSec) {
                    std::cout << "[Scenario 4] Timeout waiting: completed " << done
                              << "/" << targetDone << " ops.\n";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }

    // === PBFT client logic: send, wait, retry ===
    int sentCount = 0;
    while (!txnQueue.empty()) {
        Transaction txn = txnQueue.front();
        txnQueue.pop();

        bool enoughResponses = false;
        {
            std::lock_guard<std::mutex> lock(txnMutex);
            if (completedTxns.count(txn.id)) continue;
        }

        if (txn.retries == 0) {
            std::cout << "\n[PBFTClient] Sending txn " << txn.id << " to leader\n";

            if (scenario == 6) {
                // First attempt for scenario 6: use gRPC to leader
                json j = json::parse(txn.payload);
                int grpcLeaderPort = leaderPort + 10000; // 15001 for leader 5001
                if (!grpcSubmitRequest(grpcLeaderPort, j)) {
                    std::cout << "[PBFTClient] gRPC SubmitRequest to leader failed for " << txn.id << ".\n";
                }
            } else {
                // Existing TCP path for other scenarios
                try {
                    TcpConnection clientConn(leaderPort, false);
                    clientConn.send(txn.payload);
                    clientConn.closeConnection();
                } catch (...) {
                    std::cout << "[PBFTClient] Could not connect to leader.\n";
                }
            }

            sentCount++;
            if (scenario == 5 && sentCount == 2) {
                std::cout << "[IntegrationTest] Turning off node on port 5001 mid-transaction...\n";
                try {
                    TcpConnection nodeConn(5001, false);
                    json statusMsg = {
                        {"type", "changeServerStatus"},
                        {"server_status", 0}
                    };
                    nodeConn.send(statusMsg.dump());
                    nodeConn.closeConnection();
                } catch (...) {
                    std::cout << "[IntegrationTest] Could not connect to node 5001 to turn off.\n";
                }
            }
        } else {
            std::cout << "\n[PBFTClient] Retrying txn " << txn.id
                      << " via gRPC to all nodes (retry " << txn.retries << ")\n";

            json j = json::parse(txn.payload);

            for (int basePort : nodePorts) {
                int grpcPort = basePort + 10000;  // e.g. 5001 -> 15001
                if (!grpcSubmitRequest(grpcPort, j)) {
                    std::cout << "[PBFTClient] gRPC retry SubmitRequest to " << grpcPort
                              << " failed for " << txn.id << ".\n";
                } else {
                    std::cout << "[PBFTClient] Retried txn " << txn.id
                              << " via gRPC to node on port " << grpcPort << "\n";
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(9000)); // small delay before waiting for responses
        }

        // Wait for responses for this transaction (unchanged)
        auto waitStart = std::chrono::steady_clock::now();
        while (true) {
            {
                std::lock_guard<std::mutex> lock(txnMutex);
                if (txnResponses[txn.id] >= requiredResponses) {
                    std::cout << "[PBFTClient] Got " << txnResponses[txn.id]
                            << " responses for txn " << txn.id << "\n";
                    enoughResponses = true;
                    completedTxns.insert(txn.id);
                    break;
                }
            }
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - waitStart).count() > responseTimeoutSec) {
                std::cout << "[PBFTClient] Timeout waiting for txn " << txn.id
                        << " responses, will retry if possible...\n";
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!enoughResponses && txn.retries < maxRetries) {
            Transaction retryTxn = txn;
            retryTxn.retries++;
            txnQueue.push(retryTxn); // retry
        } else if (!enoughResponses) {
            std::cout << "[PBFTClient] Failed to get enough responses for txn "
                    << txn.id << " after " << maxRetries << " retries.\n";
        }
    }

    

    // Wait for responses for up to maxWaitSeconds, but exit early if enough responses are received
    const int maxWaitSeconds = 3;
    auto waitStart = std::chrono::steady_clock::now();
    while (true) {
        {
            std::lock_guard<std::mutex> lock(responsesMutex);
            if (responses.size() >= n*transactions.size()) break;
        }
        auto now = std::chrono::steady_clock::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - waitStart).count() > maxWaitSeconds) {
            // std::cout << "[IntegrationTest] Timeout waiting for responses.\n";
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Query balances from all nodes
    // std::cout << "\n[IntegrationTest] Querying balances from all nodes...\n";
    for (int port : nodePorts) {
        try {
            TcpConnection nodeConn(port, false);
            json query = {
                {"type", "QueryBalances"},
                {"message_sender_id", "client"},
                {"timestamp", std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count())},
                {"client_listen_port", clientListenPort}
            };
            std::string queryStr = query.dump();
            nodeConn.send(queryStr);
            nodeConn.closeConnection();
            // std::cout << "[IntegrationTest] Sent balance query to node on port " << port << "\n";
        } catch (...) {
            std::cout << "[IntegrationTest] Could not connect to node " << port << ".\n";
        }
    }
    //std::this_thread::sleep_for(std::chrono::seconds(1)); // Give time for balance queries to be processed

    // Print all received responses (already printed in listener, but you can print again if needed)
    // std::cout << "\n[IntegrationTest] All received responses:\n";
    // for (const auto& resp : responses) {
    //     std::cout << resp << std::endl;
    // }

    // Expected balances (adjust as needed)
    std::map<std::string, int> expectedBalances = initialBalances;
    for (const auto& [from, to, amount] : transactions) {
        expectedBalances[from] -= amount;
        expectedBalances[to] += amount;
    }

    // std::cout << "[IntegrationTest] Expected balances:\n";
    // for (const auto& [client, expected] : expectedBalances) {
    //     std::cout << client << ": " << expected << std::endl;
    // }

    // std::cout << "[IntegrationTest] Transactions are...\n";
    // for (const auto& [from, to, amount] : transactions) {
    //     std::cout << from << " -> " << to << ": " << amount << std::endl;
    // }
    bool allBalancesCorrect = true;

    for (const auto& resp : responses) {
        try {
            auto j = json::parse(resp);
            if (j.contains("balances")) {
                // std::cout << "[Validation] Received balances response: " << j.dump() << std::endl;
                int senderId = j["message_sender_id"].get<int>();
                std::cout << "[Validation] message_sender_id: " << senderId << std::endl;
                auto balances = j["balances"];
                for (const auto& [client, expected] : expectedBalances) {
                    if(expected==100){
                        continue;
                    }
                    if (!balances.contains(client) ) {
                        // std::cout << "[Validation] No balance for client " << client << ", expected" << expected << std::endl;
                        std::cout << "[Validation] Missing balance for client " << client << std::endl;
                        allBalancesCorrect = false;
                        continue;
                    }
                    int actual = balances[client].get<int>();
                    if (actual != expected) {
                        std::cout << "[Validation] Balance mismatch for " << client << ": expected " << expected << ", got " << actual << std::endl;
                        allBalancesCorrect = false;
                    }
                }
            }
        } catch (...) {
            // Ignore parse errors for non-balance responses
        }
    }

    if (!allBalancesCorrect) {
        std::cout << "[IntegrationTest] Balance validation failed!\n";
    } else {
        std::cout << "[IntegrationTest] All balances are correct.\n";
    }

    running = false;
    // Connect to self to unblock accept if needed
    int dummySock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in dummyAddr{};
    dummyAddr.sin_family = AF_INET;
    dummyAddr.sin_port = htons(clientListenPort);
    dummyAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    connect(dummySock, (struct sockaddr*)&dummyAddr, sizeof(dummyAddr));
    close(dummySock);

    if (serverThread.joinable()) serverThread.join();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    //std::cout << "[IntegrationTest] Time taken for " << NUM_REQUESTS << " requests: " << durationMs << " ms" << std::endl;

    // if (scenario == 4) {
    //     scenario4Bench.finishRun();
    //     scenario4Bench.printSummary();
    //     // Uncomment to save raw latencies:
    //     // scenario4Bench.exportCSV("scenario4_latencies.csv");
    // }

    std::cout << "Integration test complete.\n";
    return 0;
}
