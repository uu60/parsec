#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>


#define PORT 3307
#define BUFFER_SIZE 1024

class Client {
public:
    Client() : sock(-1) {
        setupConnection();
    }

    ~Client() {
        if (sock != -1) close(sock);
    }

    void run() {
        std::cout << "Welcome to the SMPC-DB client.\n"
                  << "Type 'exit' or 'quit' to end the session.\n" << std::endl;

        std::string command;
        while (true) {
            // Use readline to get user input with advanced line editing and history support
            char* input = readline("smpc-db> ");
            if (!input) {
                std::cout << "Goodbye!" << std::endl;
                break;  // Exit if EOF (Ctrl+D)
            }

            std::string line(input);
            free(input);  // Free the memory allocated by readline

            // Add the line to history if it's not empty
            if (!line.empty()) {
                add_history(line.c_str());
            }

            if (line == "exit" || line == "quit") {
                std::cout << "Goodbye!" << std::endl;
                break;
            }

            command += line;

            // Check if the command ends with a semicolon to indicate it's complete
            if (!command.empty() && command.back() == ';') {
                command.pop_back(); // Remove the trailing semicolon

                // Send the command to the server
                send(sock, command.c_str(), command.size(), 0);

                // Receive the response in chunks
                std::string response;
                char buffer[BUFFER_SIZE];
                int64_t bytes_received;

                while (true) {
                    bytes_received = recv(sock, buffer, BUFFER_SIZE, 0);

                    if (bytes_received < 0) {
                        std::cerr << "Error receiving data from server." << std::endl;
                        break;
                    } else if (bytes_received == 0) {
                        // No more data; server closed connection
                        break;
                    }

                    // Append the chunk to the response
                    response.append(buffer, bytes_received);

                    // If the received data is less than the buffer size, assume the end of message
                    if (bytes_received < BUFFER_SIZE) {
                        break;
                    }
                }

                std::cout << response << std::endl;

                command.clear();  // Clear the command for the next input
            } else {
                // Continue collecting lines for multi-line command
                command += " ";
            }
        }
        close(sock);
    }

private:
    int sock;
    struct sockaddr_in serv_addr;

    void setupConnection() {
        if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            std::cerr << "Socket creation failed" << std::endl;
            exit(EXIT_FAILURE);
        }

        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(PORT);

        if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
            std::cerr << "Invalid address / Address not supported" << std::endl;
            exit(EXIT_FAILURE);
        }

        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "Connection Failed" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};

int main() {
    Client client;
    client.run();
    return 0;
}