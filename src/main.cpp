#include "../include/coordination/CoordinationServer.h"
#include "../include/coordination/CoordinationUnit.h"
#include <iostream>
#include <thread>
#include <cstdlib>   // NEW for std::getenv

int main(int argc, char** argv) {
    bool agentEnabled = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--agent") agentEnabled = true;
    }

    CoordinationServer server;
    CoordinationUnit unit;
    unit.setAgentEnabled(agentEnabled);

    //change path in entity.cpp to change protocol
    server.loadConfig("config.pbft.yaml");
    server.sendStartSignal();

    // Start CoordinationUnit in a separate thread
    std::thread unitThread([&unit]() {
        unit.start();
    });

    // Remove proxy bootstrap; Entities now host gRPC themselves
    std::cout << "Main thread continues execution while server is listening..." << std::endl;
    
    // Simulate some other task
    for (int i = 0; i < 10; i++) {
        //std::cout << "Main is doing work..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(100));
    }
    std::cout << "Main thread finished work, stopping server..." << std::endl;
    server.sendStopSignal();
    unit.stop();

    // Ensure the unit thread finishes before exiting
    if (unitThread.joinable()) {
        unitThread.join();
    }

    return 0;
}
