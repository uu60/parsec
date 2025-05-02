// //
// // Created by 杜建璋 on 2024/10/31.
// //
//
// #ifndef SMPC_DATABASE_LOCALSERVER_H
// #define SMPC_DATABASE_LOCALSERVER_H
//
// #include <iostream>
// #include <cstring>
// #include <sys/socket.h>
// #include <arpa/inet.h>
// #include <unistd.h>
//
// #define PORT 3307
// #define BUFFER_SIZE 1024
//
// class LocalServer {
// private:
//     LocalServer();
//
// public:
//     ~LocalServer();
//
//     static LocalServer &getInstance();
//
//     LocalServer(const LocalServer &) = delete;
//
//     LocalServer &operator=(const LocalServer &) = delete;
//
//     void run();
//
//     void send_(std::string msg);
//
// private:
//     int server_fd, new_socket;
//     struct sockaddr_in address;
//     int addrlen = sizeof(address);
//
//     void setupServer();
//
//     std::string processSQLCommand(const std::string &command);
// };
//
//
// #endif //SMPC_DATABASE_LOCALSERVER_H
