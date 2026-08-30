#pragma once
#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <string>
#include <netinet/in.h>
#include <queue>

class TcpConnection {
public:
    TcpConnection(int port, bool isServer);
    ~TcpConnection();

    void send(const std::string& message);
    std::string receive();
    void startListening();  // New function to start a listener thread
    void stopListening();   // Graceful shutdown of thread
    void closeConnection();

private:
    int sockfd;
    int clientSock;
    int port;
    bool isServer;
    bool running;
    std::thread listenerThread;

    std::queue<std::string> messageQueue;
    std::mutex queueMutex;
    std::condition_variable queueCondVar;

    void listenForConnections();
};
