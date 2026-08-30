#include "../../../include/coordination/connections/TcpConnection.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

TcpConnection::TcpConnection(int port, bool isServer) 
    : port(port), isServer(isServer), running(false), clientSock(-1) {
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        std::cerr << "Error creating socket\n";
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = isServer ? INADDR_ANY : inet_addr("127.0.0.1");

    if (isServer) {
        if (bind(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "Bind failed\n";
            return;
        }
        listen(sockfd, 5);
    } else {
        if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "Client failed to connect\n";
            throw std::runtime_error("Client failed to connect");
        }
    }
}

TcpConnection::~TcpConnection() {
    stopListening();
    close(sockfd);
}

void TcpConnection::send(const std::string& message) {
    ::send(sockfd, message.c_str(), message.size(), 0);
}

void TcpConnection::startListening() {
    running = true;
    listenerThread = std::thread(&TcpConnection::listenForConnections, this);
}

void TcpConnection::stopListening() {
    running = false;
    if (listenerThread.joinable()) {
        listenerThread.join();
    }
}

void TcpConnection::listenForConnections() {
    while (running) {
        sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        clientSock = accept(sockfd, (struct sockaddr*)&clientAddr, &clientLen);
        if (clientSock < 0) {
            std::cerr << "Accept failed\n";
            continue;
        }

        while (running) {
            char buffer[16 * 1024] = {0}; // 16 KB buffer
            int bytesReceived = recv(clientSock, buffer, sizeof(buffer), 0);
            if (bytesReceived > 0) {
                std::string receivedMessage(buffer, bytesReceived);
                {
                    std::lock_guard<std::mutex> lock(queueMutex);
                    messageQueue.push(receivedMessage);
                }
                queueCondVar.notify_one();
            } else if (bytesReceived == 0) {
                close(clientSock);
                clientSock = -1;
                break;
            }
        }
    }
}

std::string TcpConnection::receive() {
    std::unique_lock<std::mutex> lock(queueMutex);
    queueCondVar.wait(lock, [this] { return !messageQueue.empty(); });

    std::string message = messageQueue.front();
    messageQueue.pop();
    return message;
}

void TcpConnection::closeConnection() {
    if (sockfd >= 0) {
        close(sockfd);
        sockfd = -1;
    }
}
