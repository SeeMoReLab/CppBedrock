#include "../../include/coordination/CoordinationServer.h"
#include <iostream>
#include "../../include/coordination/connections/TcpConnection.h"
#include <string>
//#include <yaml-cpp/yaml.h>

CoordinationServer::CoordinationServer() {}

void CoordinationServer::initialize() {
    std::cout << "Coordination Server initialized." << std::endl;
}

void CoordinationServer::start() {
    std::cout << "Coordination Server started." << std::endl;
}

void CoordinationServer::stop() {
    std::cout << "Coordination Server stopped." << std::endl;
}

void CoordinationServer::loadConfig(const std::string& configFile) {
    std::cout << "Loading config file" << std::endl;
    // YAML::Node config = YAML::LoadFile(configFile);
    
    // for (const auto& role : config["roles"]) {
    //     std::string roleName = role.as<std::string>();
    //     transitions[roleName] = {};

    //     for (const auto& state : config["transitions"][roleName]) {
    //         std::string stateName = state.first.as<std::string>();
    //         std::string targetState = state.second["next_state"].as<std::string>();
    //         std::string message = state.second["message"].as<std::string>();

    //         transitions[roleName][stateName] = {targetState, message};
    //     }
    // }
}

void CoordinationServer::sendStartSignal() {
    TcpConnection client(50001, false);
    client.send("start the units");
    std::cout << "Sending start signal to all coordination units." << std::endl;
}

void CoordinationServer::sendStopSignal() {
    TcpConnection client(50001, false);
    client.send("stop the units");
    std::cout << "Sending stop signal to all coordination units." << std::endl;
}
