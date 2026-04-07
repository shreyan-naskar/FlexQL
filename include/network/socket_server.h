#pragma once

#include <string>

namespace flexql {

class SocketServer {
public:
    SocketServer();
    ~SocketServer();

    bool start(int port_number, std::string &error_message);
    int accept_client(std::string &error_message);
    void close();
    int fd() const;

    static bool send_message(
        int client_fd,
        const std::string &payload,
        std::string &error_message);
    static bool receive_message(
        int client_fd,
        std::string &payload,
        std::string &error_message);

private:
    int fd_;
};

}  // namespace flexql
