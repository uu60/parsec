//
// Created by 杜建璋 on 2024/10/31.
//

#include "../../include/socket/LocalServer.h"
#include <iostream>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "../../third_party/json.hpp"
#include "../../include/dbms/SystemManager.h"
using json = nlohmann::json;

LocalServer::LocalServer() : server_fd(-1), new_socket(-1) {
    setupServer();
}

LocalServer::~LocalServer() {
    if (server_fd != -1) close(server_fd);
}

void LocalServer::setupServer() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
#ifdef SO_REUSEPORT
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEPORT failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }
#endif

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(PORT);

    socklen_t len = sizeof(address);
    if (bind(server_fd, (struct sockaddr *) &address, len) < 0) {
        perror("Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    std::cout << "Server is listening on port " << PORT << std::endl;
}

void LocalServer::run() {
    while (true) {
        // Wait for a new client connection
        if ((new_socket = accept(server_fd, (struct sockaddr *) &address, (socklen_t *) &addrlen)) < 0) {
            perror("Accept failed");
            return;
        }

        while (true) {
            std::string command;
            char buffer[BUFFER_SIZE] = {0};
            int64_t valread;

            // Receive message in chunks
            while ((valread = read(new_socket, buffer, BUFFER_SIZE)) > 0) {
                // Append the chunk to the command string
                command.append(buffer, valread);

                // If the received data is less than BUFFER_SIZE, assume the end of the message
                if (valread < BUFFER_SIZE) {
                    break;
                }
            }

            if (valread <= 0) {
                // Client disconnected or an error occurred
                break;
            }

            // Check if the command is "exit" (case-insensitive)
            if (strcasecmp(command.c_str(), "exit") == 0) {
                json j;
                j["type"] = "exit";
                SystemManager::notifyServersSync(j);
                break;
            }

            // Process the complete command
            SystemManager::getInstance().clientExecute(command);
        }

        // Close the socket for the current client, allowing the server to accept a new connection
        close(new_socket);
    }

    // Close the server socket when the server shuts down
    close(server_fd);
}

std::string LocalServer::processSQLCommand(const std::string &command) {
    return "Server processed command: " + command;
}

LocalServer &LocalServer::getInstance() {
    static LocalServer localServer;
    return localServer;
}

void LocalServer::send_(std::string msg) {
    send(new_socket, msg.c_str(), msg.size(), 0);
}
