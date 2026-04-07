#include "network/socket_client.h"

#include <arpa/inet.h>
#include <netdb.h>
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

SocketClient::SocketClient() : fd_(-1) {}

SocketClient::~SocketClient() {
    close();
}

bool SocketClient::connect_to(
    const std::string &host_name,
    int port_number,
    std::string &failure_reason) {
    close();

    struct addrinfo address_hints {};
    address_hints.ai_family = AF_UNSPEC;
    address_hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *address_list = nullptr;
    const std::string service_name = std::to_string(port_number);
    if (::getaddrinfo(host_name.c_str(), service_name.c_str(), &address_hints, &address_list) != 0) {
        failure_reason = "failed to resolve host";
        return false;
    }

    for (struct addrinfo *candidate = address_list; candidate != nullptr; candidate = candidate->ai_next) {
        fd_ = ::socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (fd_ < 0) {
            continue;
        }
        if (::connect(fd_, candidate->ai_addr, candidate->ai_addrlen) == 0) {
            ::freeaddrinfo(address_list);
            int disable_nagle = 1;
            ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &disable_nagle, sizeof(disable_nagle));
            return true;
        }
        ::close(fd_);
        fd_ = -1;
    }

    ::freeaddrinfo(address_list);
    failure_reason = "failed to connect";
    return false;
}

bool SocketClient::send_message(const std::string &payload, std::string &failure_reason) {
    const std::uint32_t network_length = htonl(static_cast<std::uint32_t>(payload.size()));
    // Combine length prefix + payload into one send to avoid two small TCP segments.
    std::string frame(sizeof(network_length) + payload.size(), '\0');
    std::memcpy(frame.data(), &network_length, sizeof(network_length));
    std::memcpy(frame.data() + sizeof(network_length), payload.data(), payload.size());
    return send_all(fd_, frame.data(), frame.size(), failure_reason);
}

bool SocketClient::receive_message(std::string &payload, std::string &failure_reason) {
    std::uint32_t network_length = 0;
    if (!recv_all(fd_, reinterpret_cast<char *>(&network_length), sizeof(network_length), failure_reason)) {
        return false;
    }

    const std::uint32_t payload_size = ntohl(network_length);
    payload.assign(payload_size, '\0');
    return recv_all(fd_, payload.data(), payload_size, failure_reason);
}

void SocketClient::close() {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        fd_ = -1;
    }
}

bool SocketClient::is_open() const {
    return fd_ >= 0;
}

}  // namespace flexql
