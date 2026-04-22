#include "../include/coordination/CoordinationServer.h"
#include "../include/coordination/CoordinationUnit.h"
#include "../include/core/Entity.h"
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <thread>
#include <csignal>
#include <condition_variable>
#include <mutex>

static std::condition_variable g_cv;
static std::mutex g_mu;
static bool g_stop = false;

int main(int argc, char** argv) {
    bool agentEnabled = false;
    int nodeId = -1;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--agent") agentEnabled = true;
        if (std::string(argv[i]) == "--node-id" && i + 1 < argc)
            nodeId = std::stoi(argv[++i]);
    }

    if (nodeId != -1) {
        // Single node mode.
        YAML::Node config = YAML::LoadFile("../config/config.entities.yaml");
        std::unique_ptr<Entity> entity;
        for (const auto& e : config["entities"]) {
            if (e["id"].as<int>() == nodeId) {
                entity = std::make_unique<Entity>(
                    e["role"].as<std::string>(),
                    nodeId,
                    e["peers"].as<std::vector<int>>(),
                    e["byzantine"].as<bool>()
                );
                break;
            }
        }
        if (!entity) {
            std::cerr << "Node ID " << nodeId << " not found in config.entities.yaml\n";
            return 1;
        }
        entity->setAgentEnabled(agentEnabled);
        entity->loadByzantineSchedule("../config/config.sbft.byzantine.yaml");
        entity->start();

        signal(SIGINT, [](int) {
            std::unique_lock<std::mutex> lk(g_mu);
            g_stop = true;
            g_cv.notify_all();
        });
        std::unique_lock<std::mutex> lk(g_mu);
        g_cv.wait(lk, [] { return g_stop; });

        entity->stop();
        return 0;
    }

    // All-in-one mode (original behaviour)
    CoordinationServer server;
    CoordinationUnit unit;
    unit.setAgentEnabled(agentEnabled);

    server.loadConfig("config.pbft.yaml");
    server.sendStartSignal();

    std::thread unitThread([&unit]() {
        unit.start();
    });

    std::cout << "Main thread continues execution while server is listening..." << std::endl;
    for (int i = 0; i < 10; i++) {
        std::this_thread::sleep_for(std::chrono::seconds(100));
    }
    std::cout << "Main thread finished work, stopping server..." << std::endl;
    server.sendStopSignal();
    unit.stop();

    if (unitThread.joinable()) {
        unitThread.join();
    }

    return 0;
}
