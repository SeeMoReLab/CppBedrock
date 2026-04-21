#include "../../include/coordination/CoordinationUnit.h"
#include "../../include/core/state/StateMachine.h"
#include "../../include/core/Entity.h"
#include <nlohmann/json.hpp>
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <memory>

using json = nlohmann::json;

CoordinationUnit::CoordinationUnit()
    : server(50001, true), stateMachine(), entityState("Replica", "Idle", 0, 0), entitiesStarted(false) {
}

void CoordinationUnit::initialize() {
    std::cout << "Coordination Unit initialized." << std::endl;
}

void CoordinationUnit::start() {
    std::cout << "Coordination Unit started." << std::endl;
    server.startListening(); // Start TCP listener in a separate thread
    running = true;
    // Start a new thread to continuously receive messages
    std::thread listeningThread = std::thread([this]() {
        while (running) {  // Ensure we can stop the loop when needed
            receiveMessage();
        }
    });

    listeningThread.detach(); // Run independently
}

void CoordinationUnit::stop() {
    std::cout << "Coordination Unit stopped." << std::endl;
    running = false; // Ensure the loop in start() exits
    server.stopListening(); // Stop the listener thread gracefully
}

void CoordinationUnit::initializeEntities() {
    std::cout << "Initializing entities in the coordination unit." << std::endl;
}

void CoordinationUnit::receiveMessage() {
    std::string receivedMessage = server.receive();
    std::cout << "Coordination Unit received message: " << receivedMessage << std::endl;

    if (receivedMessage == "start the units" && !entitiesStarted) {
        entitiesStarted = true;
        YAML::Node config = YAML::LoadFile("../config/config.entities.yaml");
        const auto& entitiesNode = config["entities"];
        std::vector<std::unique_ptr<Entity>> tempEntities;
        for (const auto& entityNode : entitiesNode) {
            int id = entityNode["id"].as<int>();
            std::string role = entityNode["role"].as<std::string>();
            
            std::vector<int> peers = entityNode["peers"].as<std::vector<int>>();
            bool byzantine = entityNode["byzantine"].as<bool>(); // default to false if missing
            tempEntities.push_back(std::make_unique<Entity>(role, id, peers, byzantine));
        }
        // Propagate agent-enabled flag
        for (auto& entity : tempEntities) {
            entity->setAgentEnabled(agentEnabled_);
        }
        // Load byzantine schedule (SBFT-specific; silently no-ops if file absent)
        for (auto& entity : tempEntities) {
            entity->loadByzantineSchedule("../config/config.sbft.byzantine.yaml");
        }
        // Start all entities
        for (auto& entity : tempEntities) {
            entity->start();
        }
        // Move to member variables if needed
        entities.clear();
        for (auto& entity : tempEntities) {
            entities.push_back(std::move(entity));
        }
        std::cout << "[CoordinationUnit] Entities started from config.\n";
    } else if (receivedMessage == "stop the units" && entitiesStarted) {
        for (auto& entity : entities) {
            entity->stop();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        entitiesStarted = false;
        std::cout << "[CoordinationUnit] Entities stopped.\n";
    } else if (entitiesStarted) {
        // Assume any other message is a client request (JSON string)
        try {
            nlohmann::json j = nlohmann::json::parse(receivedMessage);
            Message msg(receivedMessage);
            entities[0]->handleEvent(&msg, &entities[0]->getState());
            std::cout << "[CoordinationUnit] Forwarded client request to leader.\n";
        } catch (...) {
            std::cout << "[CoordinationUnit] Received non-JSON message while running.\n";
        }
    }
}


