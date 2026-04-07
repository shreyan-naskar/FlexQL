#include "network/socket_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

namespace flexql {
namespace {

bool send_all(
    int socket_fd,
    const char *buffer,
    std::size_t byte_count,
    std::string &failure_reason) {
    std::size_t bytes_sent = 0;
    while (bytes_sent < byte_count) {
        const ssize_t sent_now = ::send(socket_fd, buffer + bytes_sent, byte_count - bytes_sent, 0);
        if (sent_now <= 0) {
            failure_reason = std::strerror(errno);
            return false;
        }
        bytes_sent += static_cast<std::size_t>(sent_now);
    }
    return true;
}

bool recv_all(
    int socket_fd,
    char *buffer,
    std::size_t byte_count,
    std::string &failure_reason) {
    std::size_t bytes_received = 0;
    while (bytes_received < byte_count) {
        const ssize_t received_now = ::recv(
            socket_fd,
            buffer + bytes_received,
            byte_count - bytes_received,
            0);
        if (received_now <= 0) {
            failure_reason = received_now == 0 ? "connection closed" : std::strerror(errno);
            return false;
        }
        bytes_received += static_cast<std::size_t>(received_now);
    }
    return true;
}

}  // namespace

SocketServer::SocketServer() : fd_(-1) {}

SocketServer::~SocketServer() {
    close();
}

bool SocketServer::start(int port_number, std::string &failure_reason) {
    close();

    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        failure_reason = std::strerror(errno);
        return false;
    }

    int reuse_flag = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse_flag, sizeof(reuse_flag));

    sockaddr_in listen_address {};
    listen_address.sin_family = AF_INET;
    listen_address.sin_addr.s_addr = htonl(INADDR_ANY);
    listen_address.sin_port = htons(static_cast<std::uint16_t>(port_number));

    if (::bind(fd_, reinterpret_cast<sockaddr *>(&listen_address), sizeof(listen_address)) < 0) {
        failure_reason = std::strerror(errno);
        close();
        return false;
    }

    if (::listen(fd_, 32) < 0) {
        failure_reason = std::strerror(errno);
        close();
        return false;
    }

    return true;
}

int SocketServer::accept_client(std::string &failure_reason) {
    const int accepted_fd = ::accept(fd_, nullptr, nullptr);
    if (accepted_fd < 0) {
        failure_reason = std::strerror(errno);
        return accepted_fd;
    }

    int disable_nagle = 1;
    ::setsockopt(accepted_fd, IPPROTO_TCP, TCP_NODELAY, &disable_nagle, sizeof(disable_nagle));
    return accepted_fd;
}

void SocketServer::close() {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        fd_ = -1;
    }
}

int SocketServer::fd() const {
    return fd_;
}

bool SocketServer::send_message(
    int client_fd,
    const std::string &payload,
    std::string &failure_reason) {
    const std::uint32_t network_length = htonl(static_cast<std::uint32_t>(payload.size()));
    // Combine length prefix + payload into one send to avoid two small TCP segments.
    std::string frame(sizeof(network_length) + payload.size(), '\0');
    std::memcpy(frame.data(), &network_length, sizeof(network_length));
    std::memcpy(frame.data() + sizeof(network_length), payload.data(), payload.size());
    return send_all(client_fd, frame.data(), frame.size(), failure_reason);
}

bool SocketServer::receive_message(
    int client_fd,
    std::string &payload,
    std::string &failure_reason) {
    std::uint32_t network_length = 0;
    if (!recv_all(client_fd, reinterpret_cast<char *>(&network_length), sizeof(network_length), failure_reason)) {
        return false;
    }

    const std::uint32_t payload_size = ntohl(network_length);
    payload.assign(payload_size, '\0');
    return recv_all(client_fd, payload.data(), payload_size, failure_reason);
}

}  // namespace flexql
