#pragma once

#include <string>

namespace flexql {

class SocketClient {
public:
    SocketClient();
    ~SocketClient();

    bool connect_to(const std::string &host_name, int port_number, std::string &error_message);
    bool send_message(const std::string &payload, std::string &error_message);
    bool receive_message(std::string &payload, std::string &error_message);
    void close();
    bool is_open() const;

private:
    int fd_;
};

}  // namespace flexql
