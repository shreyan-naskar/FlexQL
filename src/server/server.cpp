#include <arpa/inet.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cache/lru_cache.h"
#include "concurrency/thread_pool.h"
#include "network/event_loop.h"
#include "network/socket_server.h"
#include "query/executor.h"
#include "utils/helpers.h"
#include "utils/logger.h"

namespace flexql {
namespace {

constexpr std::size_t kIoUringEntries = 256;
constexpr std::size_t kMaxIoUringMessageSize = 16 * 1024 * 1024;
constexpr int kEventLoopBatchSize = 64;

std::string build_response(const QueryResult &query_result) {
    if (!query_result.success) {
        return "ERR\t" + query_result.message + "\n";
    }
    if (query_result.columns.empty()) {
        return "OK\t" + query_result.message + "\n";
    }

    std::string response_frame = "RESULT\t" +
        std::to_string(query_result.columns.size()) + '\t' +
        std::to_string(query_result.rows.size()) + "\n";
    response_frame += join(query_result.columns, "\t") + "\n";
    for (const auto &row : query_result.rows) {
        response_frame += join(row, "\t") + "\n";
    }
    return response_frame;
}

class RequestProcessor {
public:
    explicit RequestProcessor(std::string root_path)
        : executor_(std::move(root_path), std::make_shared<LruCache>(512)) {}

    std::string process(const std::string &payload, std::string &current_database) {
        QueryResult query_result = execute_message(payload, current_database);
        return build_response(query_result);
    }

private:
    QueryResult execute_message(const std::string &payload, std::string &current_database) {
        if (payload.rfind("MSG_BATCH_INSERT\t", 0) != 0) {
            return executor_.execute_sql(payload, current_database);
        }

        std::stringstream input_stream(payload);
        std::string header_line;
        std::getline(input_stream, header_line);
        const auto header_parts = split(header_line, '\t');
        if (header_parts.size() != 2) {
            return {false, "invalid batch header", {}, {}};
        }

        int expected_count = 0;
        try {
            expected_count = std::stoi(header_parts[1]);
        } catch (...) {
            return {false, "invalid batch size", {}, {}};
        }

        std::vector<std::string> batch_statements;
        std::string current_line;
        while (std::getline(input_stream, current_line)) {
            if (!trim(current_line).empty()) {
                batch_statements.push_back(current_line);
            }
        }
        if (static_cast<int>(batch_statements.size()) != expected_count) {
            return {false, "batch size mismatch", {}, {}};
        }
        return executor_.execute_batch_insert(batch_statements, current_database);
    }

    Executor executor_;
};

class FlexQLServer {
public:
    explicit FlexQLServer(std::string root_path) : processor_(std::move(root_path)), pool_() {}

    int run(int port_number) {
        std::string failure_reason;
        if (!server_.start(port_number, failure_reason)) {
            Logger::error("server start failed: " + failure_reason);
            return 1;
        }
        Logger::info(
            "FlexQL server listening on port " + std::to_string(port_number) + " using thread pool");

        while (running_) {
            const int client_fd = server_.accept_client(failure_reason);
            if (client_fd < 0) {
                Logger::error("accept failed: " + failure_reason);
                continue;
            }
            pool_.enqueue([this, client_fd]() { handle_client(client_fd); });
        }
        return 0;
    }

private:
    void handle_client(int client_fd) {
        Logger::info("client connected");
        std::string payload;
        std::string failure_reason;
        std::string current_database = "default";
        while (SocketServer::receive_message(client_fd, payload, failure_reason)) {
            const std::string response_frame = processor_.process(payload, current_database);
            if (!SocketServer::send_message(client_fd, response_frame, failure_reason)) {
                break;
            }
        }
        ::close(client_fd);
        Logger::info("client disconnected");
    }

    SocketServer server_;
    RequestProcessor processor_;
    ThreadPool pool_;
    std::atomic<bool> running_ {true};
};

bool set_nonblocking(int file_descriptor, std::string &failure_reason) {
    const int flags = ::fcntl(file_descriptor, F_GETFL, 0);
    if (flags < 0) {
        failure_reason = std::strerror(errno);
        return false;
    }
    if (::fcntl(file_descriptor, F_SETFL, flags | O_NONBLOCK) < 0) {
        failure_reason = std::strerror(errno);
        return false;
    }
    return true;
}

class EpollServer {
public:
    explicit EpollServer(std::string root) : processor_(std::move(root)) {}

    int run(int port) {
        std::string error;
        if (!server_.start(port, error)) {
            Logger::error("server start failed: " + error);
            return 1;
        }
        if (!set_nonblocking(server_.fd(), error)) {
            Logger::error("failed to make listening socket nonblocking: " + error);
            return 1;
        }
        if (!loop_.open(error)) {
            Logger::error("epoll init failed: " + error);
            return 1;
        }
        if (!loop_.add(server_.fd(), EPOLLIN, error)) {
            Logger::error("epoll add listen fd failed: " + error);
            return 1;
        }

        Logger::info("FlexQL server listening on port " + std::to_string(port) + " using epoll");
        std::vector<EventLoopEvent> events(kEventLoopBatchSize);
        while (running_) {
            const int ready = loop_.wait(events, -1, error);
            if (ready < 0) {
                Logger::error("epoll wait failed: " + error);
                return 1;
            }
            for (int i = 0; i < ready; ++i) {
                handle_event(events[static_cast<std::size_t>(i)]);
            }
        }
        return 0;
    }

private:
    struct ClientState {
        int fd = -1;
        std::string current_database = "default";
        std::array<char, sizeof(std::uint32_t)> length_buffer {};
        std::size_t length_received = 0;
        std::uint32_t payload_length = 0;
        std::string payload;
        std::size_t payload_received = 0;
        std::string response_frame;
        std::size_t response_sent = 0;
    };

    void handle_event(const EventLoopEvent &event) {
        if (event.fd == server_.fd()) {
            accept_pending_clients();
            return;
        }

        if ((event.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U) {
            close_client(event.fd);
            return;
        }

        auto it = clients_.find(event.fd);
        if (it == clients_.end()) {
            return;
        }

        if ((event.events & EPOLLIN) != 0U && !handle_client_read(*it->second)) {
            close_client(event.fd);
            return;
        }
        if ((event.events & EPOLLOUT) != 0U && !handle_client_write(*it->second)) {
            close_client(event.fd);
        }
    }

    void accept_pending_clients() {
        while (true) {
            std::string error;
            int client_fd = server_.accept_client(error);
            if (client_fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return;
                }
                Logger::error("epoll accept failed: " + error);
                return;
            }
            if (!set_nonblocking(client_fd, error)) {
                Logger::error("failed to make client socket nonblocking: " + error);
                ::close(client_fd);
                continue;
            }

            auto client = std::make_unique<ClientState>();
            client->fd = client_fd;
            if (!loop_.add(client_fd, EPOLLIN | EPOLLRDHUP, error)) {
                Logger::error("epoll add client failed: " + error);
                ::close(client_fd);
                continue;
            }
            clients_[client_fd] = std::move(client);
            Logger::info("client connected");
        }
    }

    bool handle_client_read(ClientState &client) {
        while (true) {
            if (client.payload_length == 0) {
                const ssize_t rc = ::recv(
                    client.fd,
                    client.length_buffer.data() + client.length_received,
                    sizeof(std::uint32_t) - client.length_received,
                    0);
                if (rc == 0) {
                    return false;
                }
                if (rc < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        return true;
                    }
                    return false;
                }

                client.length_received += static_cast<std::size_t>(rc);
                if (client.length_received < sizeof(std::uint32_t)) {
                    return true;
                }

                std::uint32_t network_length = 0;
                std::memcpy(&network_length, client.length_buffer.data(), sizeof(network_length));
                client.payload_length = ntohl(network_length);
                client.length_received = 0;
                if (client.payload_length == 0 || client.payload_length > kMaxIoUringMessageSize) {
                    return false;
                }
                client.payload.assign(client.payload_length, '\0');
                client.payload_received = 0;
            }

            const ssize_t rc = ::recv(
                client.fd,
                client.payload.data() + client.payload_received,
                client.payload_length - client.payload_received,
                0);
            if (rc == 0) {
                return false;
            }
            if (rc < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return true;
                }
                return false;
            }

            client.payload_received += static_cast<std::size_t>(rc);
            if (client.payload_received < client.payload_length) {
                return true;
            }

            const std::string response = processor_.process(client.payload, client.current_database);
            const std::uint32_t response_length = htonl(static_cast<std::uint32_t>(response.size()));
            client.response_frame.assign(sizeof(response_length), '\0');
            std::memcpy(client.response_frame.data(), &response_length, sizeof(response_length));
            client.response_frame += response;
            client.response_sent = 0;

            client.payload.clear();
            client.payload_length = 0;
            client.payload_received = 0;

            std::string error;
            if (!loop_.modify(client.fd, EPOLLOUT | EPOLLRDHUP, error)) {
                Logger::error("epoll modify to writable failed: " + error);
                return false;
            }
            return true;
        }
    }

    bool handle_client_write(ClientState &client) {
        while (client.response_sent < client.response_frame.size()) {
            const ssize_t rc = ::send(
                client.fd,
                client.response_frame.data() + client.response_sent,
                client.response_frame.size() - client.response_sent,
                0);
            if (rc == 0) {
                return false;
            }
            if (rc < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return true;
                }
                return false;
            }
            client.response_sent += static_cast<std::size_t>(rc);
        }

        client.response_frame.clear();
        client.response_sent = 0;
        std::string error;
        if (!loop_.modify(client.fd, EPOLLIN | EPOLLRDHUP, error)) {
            Logger::error("epoll modify to readable failed: " + error);
            return false;
        }
        return true;
    }

    void close_client(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) {
            return;
        }
        std::string error;
        loop_.remove(fd, error);
        ::close(fd);
        clients_.erase(it);
        Logger::info("client disconnected");
    }

    SocketServer server_;
    EventLoop loop_;
    RequestProcessor processor_;
    std::unordered_map<int, std::unique_ptr<ClientState>> clients_;
    std::atomic<bool> running_ {true};
};

class IoUringRing {
public:
    ~IoUringRing() {
        close();
    }

    bool init(unsigned entries, std::string &error) {
        io_uring_params params {};
        ring_fd_ = static_cast<int>(::syscall(SYS_io_uring_setup, entries, &params));
        if (ring_fd_ < 0) {
            error = std::strerror(errno);
            return false;
        }

        sq_ring_size_ = params.sq_off.array + params.sq_entries * sizeof(unsigned);
        cq_ring_size_ = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
        if (params.features & IORING_FEAT_SINGLE_MMAP) {
            if (cq_ring_size_ > sq_ring_size_) {
                sq_ring_size_ = cq_ring_size_;
            }
            cq_ring_size_ = sq_ring_size_;
        }

        sq_ring_ptr_ = ::mmap(nullptr, sq_ring_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQ_RING);
        if (sq_ring_ptr_ == MAP_FAILED) {
            error = std::strerror(errno);
            close();
            return false;
        }

        if (params.features & IORING_FEAT_SINGLE_MMAP) {
            cq_ring_ptr_ = sq_ring_ptr_;
        } else {
            cq_ring_ptr_ = ::mmap(nullptr, cq_ring_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_CQ_RING);
            if (cq_ring_ptr_ == MAP_FAILED) {
                error = std::strerror(errno);
                close();
                return false;
            }
        }

        sqes_size_ = params.sq_entries * sizeof(io_uring_sqe);
        sqes_ = static_cast<io_uring_sqe *>(
            ::mmap(nullptr, sqes_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQES));
        if (sqes_ == MAP_FAILED) {
            sqes_ = nullptr;
            error = std::strerror(errno);
            close();
            return false;
        }

        sq_head_ = reinterpret_cast<unsigned *>(static_cast<char *>(sq_ring_ptr_) + params.sq_off.head);
        sq_tail_ = reinterpret_cast<unsigned *>(static_cast<char *>(sq_ring_ptr_) + params.sq_off.tail);
        sq_ring_mask_ = reinterpret_cast<unsigned *>(static_cast<char *>(sq_ring_ptr_) + params.sq_off.ring_mask);
        sq_ring_entries_ = reinterpret_cast<unsigned *>(static_cast<char *>(sq_ring_ptr_) + params.sq_off.ring_entries);
        sq_flags_ = reinterpret_cast<unsigned *>(static_cast<char *>(sq_ring_ptr_) + params.sq_off.flags);
        sq_array_ = reinterpret_cast<unsigned *>(static_cast<char *>(sq_ring_ptr_) + params.sq_off.array);

        cq_head_ = reinterpret_cast<unsigned *>(static_cast<char *>(cq_ring_ptr_) + params.cq_off.head);
        cq_tail_ = reinterpret_cast<unsigned *>(static_cast<char *>(cq_ring_ptr_) + params.cq_off.tail);
        cq_ring_mask_ = reinterpret_cast<unsigned *>(static_cast<char *>(cq_ring_ptr_) + params.cq_off.ring_mask);
        cqes_ = reinterpret_cast<io_uring_cqe *>(static_cast<char *>(cq_ring_ptr_) + params.cq_off.cqes);
        return true;
    }

    void close() {
        if (sqes_ != nullptr) {
            ::munmap(sqes_, sqes_size_);
            sqes_ = nullptr;
        }
        if (cq_ring_ptr_ != nullptr && cq_ring_ptr_ != MAP_FAILED && cq_ring_ptr_ != sq_ring_ptr_) {
            ::munmap(cq_ring_ptr_, cq_ring_size_);
            cq_ring_ptr_ = nullptr;
        }
        if (sq_ring_ptr_ != nullptr && sq_ring_ptr_ != MAP_FAILED) {
            ::munmap(sq_ring_ptr_, sq_ring_size_);
            sq_ring_ptr_ = nullptr;
            cq_ring_ptr_ = nullptr;
        }
        if (ring_fd_ >= 0) {
            ::close(ring_fd_);
            ring_fd_ = -1;
        }
    }

    io_uring_sqe *get_sqe() {
        const unsigned head = *sq_head_;
        const unsigned tail = *sq_tail_;
        if (tail - head >= *sq_ring_entries_) {
            return nullptr;
        }

        const unsigned index = tail & *sq_ring_mask_;
        io_uring_sqe *sqe = &sqes_[index];
        std::memset(sqe, 0, sizeof(*sqe));
        sq_array_[index] = index;
        *sq_tail_ = tail + 1;
        return sqe;
    }

    bool submit(unsigned wait_for, std::string &error) {
        const unsigned to_submit = *sq_tail_ - *sq_head_;
        unsigned flags = 0;
        if (wait_for > 0) {
            flags |= IORING_ENTER_GETEVENTS;
        }
        if ((*sq_flags_ & IORING_SQ_NEED_WAKEUP) != 0U) {
            flags |= IORING_ENTER_SQ_WAKEUP;
        }

        const int rc = static_cast<int>(::syscall(SYS_io_uring_enter, ring_fd_, to_submit, wait_for, flags, nullptr, 0));
        if (rc < 0) {
            error = std::strerror(errno);
            return false;
        }
        return true;
    }

    io_uring_cqe *peek_cqe() {
        const unsigned head = *cq_head_;
        if (head == *cq_tail_) {
            return nullptr;
        }
        return &cqes_[head & *cq_ring_mask_];
    }

    void advance_cqe() {
        *cq_head_ = *cq_head_ + 1;
    }

private:
    int ring_fd_ = -1;
    void *sq_ring_ptr_ = nullptr;
    void *cq_ring_ptr_ = nullptr;
    io_uring_sqe *sqes_ = nullptr;
    io_uring_cqe *cqes_ = nullptr;

    unsigned *sq_head_ = nullptr;
    unsigned *sq_tail_ = nullptr;
    unsigned *sq_ring_mask_ = nullptr;
    unsigned *sq_ring_entries_ = nullptr;
    unsigned *sq_flags_ = nullptr;
    unsigned *sq_array_ = nullptr;

    unsigned *cq_head_ = nullptr;
    unsigned *cq_tail_ = nullptr;
    unsigned *cq_ring_mask_ = nullptr;

    std::size_t sq_ring_size_ = 0;
    std::size_t cq_ring_size_ = 0;
    std::size_t sqes_size_ = 0;
};

class IoUringServer {
public:
    explicit IoUringServer(std::string root) : processor_(std::move(root)) {}

    int run(int port) {
        std::string error;
        if (!server_.start(port, error)) {
            Logger::error("server start failed: " + error);
            return 1;
        }
        if (!ring_.init(static_cast<unsigned>(kIoUringEntries), error)) {
            Logger::error("io_uring init failed: " + error);
            return 1;
        }

        Logger::info("FlexQL server listening on port " + std::to_string(port) + " using io_uring");
        if (!submit_accept(error)) {
            Logger::error("io_uring accept submit failed: " + error);
            return 1;
        }

        while (running_) {
            if (!ring_.submit(1, error)) {
                Logger::error("io_uring enter failed: " + error);
                return 1;
            }
            process_completions();
        }
        return 0;
    }

private:
    enum class OperationType {
        Accept,
        RecvLength,
        RecvPayload,
        SendFrame
    };

    struct ClientState {
        int fd = -1;
        std::string current_database = "default";
        std::array<char, sizeof(std::uint32_t)> length_buffer {};
        std::size_t length_received = 0;
        std::uint32_t payload_length = 0;
        std::string payload;
        std::size_t payload_received = 0;
        std::string response_frame;
        std::size_t response_sent = 0;
    };

    struct PendingOperation {
        OperationType type = OperationType::Accept;
        ClientState *client = nullptr;
        sockaddr_storage address {};
        socklen_t address_length = sizeof(sockaddr_storage);
    };

    bool submit_accept(std::string &error) {
        auto *operation = new PendingOperation();
        operation->type = OperationType::Accept;

        io_uring_sqe *sqe = ring_.get_sqe();
        if (sqe == nullptr) {
            delete operation;
            error = "submission queue is full";
            return false;
        }
        sqe->opcode = IORING_OP_ACCEPT;
        sqe->fd = server_.fd();
        sqe->addr = reinterpret_cast<__u64>(&operation->address);
        sqe->off = reinterpret_cast<__u64>(&operation->address_length);
        sqe->user_data = reinterpret_cast<__u64>(operation);
        return true;
    }

    bool submit_recv_length(ClientState *client, std::string &error) {
        auto *operation = new PendingOperation();
        operation->type = OperationType::RecvLength;
        operation->client = client;

        io_uring_sqe *sqe = ring_.get_sqe();
        if (sqe == nullptr) {
            delete operation;
            error = "submission queue is full";
            return false;
        }
        sqe->opcode = IORING_OP_RECV;
        sqe->fd = client->fd;
        sqe->addr = reinterpret_cast<__u64>(client->length_buffer.data() + client->length_received);
        sqe->len = static_cast<__u32>(sizeof(std::uint32_t) - client->length_received);
        sqe->user_data = reinterpret_cast<__u64>(operation);
        return true;
    }

    bool submit_recv_payload(ClientState *client, std::string &error) {
        auto *operation = new PendingOperation();
        operation->type = OperationType::RecvPayload;
        operation->client = client;

        io_uring_sqe *sqe = ring_.get_sqe();
        if (sqe == nullptr) {
            delete operation;
            error = "submission queue is full";
            return false;
        }
        sqe->opcode = IORING_OP_RECV;
        sqe->fd = client->fd;
        sqe->addr = reinterpret_cast<__u64>(client->payload.data() + client->payload_received);
        sqe->len = static_cast<__u32>(client->payload_length - client->payload_received);
        sqe->user_data = reinterpret_cast<__u64>(operation);
        return true;
    }

    bool submit_send_frame(ClientState *client, std::string &error) {
        auto *operation = new PendingOperation();
        operation->type = OperationType::SendFrame;
        operation->client = client;

        io_uring_sqe *sqe = ring_.get_sqe();
        if (sqe == nullptr) {
            delete operation;
            error = "submission queue is full";
            return false;
        }
        sqe->opcode = IORING_OP_SEND;
        sqe->fd = client->fd;
        sqe->addr = reinterpret_cast<__u64>(client->response_frame.data() + client->response_sent);
        sqe->len = static_cast<__u32>(client->response_frame.size() - client->response_sent);
        sqe->user_data = reinterpret_cast<__u64>(operation);
        return true;
    }

    void close_client(ClientState *client) {
        if (client == nullptr) {
            return;
        }
        if (client->fd >= 0) {
            ::close(client->fd);
            client->fd = -1;
        }
        delete client;
        Logger::info("client disconnected");
    }

    void process_completions() {
        while (io_uring_cqe *cqe = ring_.peek_cqe()) {
            auto *operation = reinterpret_cast<PendingOperation *>(cqe->user_data);
            const int result = cqe->res;
            ring_.advance_cqe();

            switch (operation->type) {
                case OperationType::Accept:
                    handle_accept(operation, result);
                    break;
                case OperationType::RecvLength:
                    handle_recv_length(operation, result);
                    break;
                case OperationType::RecvPayload:
                    handle_recv_payload(operation, result);
                    break;
                case OperationType::SendFrame:
                    handle_send_frame(operation, result);
                    break;
            }
            delete operation;
        }
    }

    void handle_accept(PendingOperation *, int result) {
        std::string error;
        if (!submit_accept(error)) {
            Logger::error("io_uring resubmit accept failed: " + error);
            running_ = false;
            return;
        }

        if (result < 0) {
            Logger::error("io_uring accept failed: " + std::string(std::strerror(-result)));
            return;
        }

        auto *client = new ClientState();
        client->fd = result;
        Logger::info("client connected");
        if (!submit_recv_length(client, error)) {
            Logger::error("io_uring submit recv length failed: " + error);
            close_client(client);
        }
    }

    void handle_recv_length(PendingOperation *operation, int result) {
        ClientState *client = operation->client;
        if (result <= 0) {
            close_client(client);
            return;
        }

        client->length_received += static_cast<std::size_t>(result);
        if (client->length_received < sizeof(std::uint32_t)) {
            std::string error;
            if (!submit_recv_length(client, error)) {
                Logger::error("io_uring resubmit recv length failed: " + error);
                close_client(client);
            }
            return;
        }

        std::uint32_t network_length = 0;
        std::memcpy(&network_length, client->length_buffer.data(), sizeof(network_length));
        client->payload_length = ntohl(network_length);
        client->length_received = 0;

        if (client->payload_length > kMaxIoUringMessageSize) {
            close_client(client);
            return;
        }
        if (client->payload_length == 0) {
            close_client(client);
            return;
        }

        client->payload.assign(client->payload_length, '\0');
        client->payload_received = 0;
        std::string error;
        if (!submit_recv_payload(client, error)) {
            Logger::error("io_uring submit recv payload failed: " + error);
            close_client(client);
        }
    }

    void handle_recv_payload(PendingOperation *operation, int result) {
        ClientState *client = operation->client;
        if (result <= 0) {
            close_client(client);
            return;
        }

        client->payload_received += static_cast<std::size_t>(result);
        if (client->payload_received < client->payload_length) {
            std::string error;
            if (!submit_recv_payload(client, error)) {
                Logger::error("io_uring resubmit recv payload failed: " + error);
                close_client(client);
            }
            return;
        }

        const std::string response = processor_.process(client->payload, client->current_database);
        const std::uint32_t response_length = htonl(static_cast<std::uint32_t>(response.size()));
        client->response_frame.assign(sizeof(response_length), '\0');
        std::memcpy(client->response_frame.data(), &response_length, sizeof(response_length));
        client->response_frame += response;
        client->response_sent = 0;
        client->payload.clear();
        client->payload_received = 0;
        client->payload_length = 0;

        std::string error;
        if (!submit_send_frame(client, error)) {
            Logger::error("io_uring submit send failed: " + error);
            close_client(client);
        }
    }

    void handle_send_frame(PendingOperation *operation, int result) {
        ClientState *client = operation->client;
        if (result <= 0) {
            close_client(client);
            return;
        }

        client->response_sent += static_cast<std::size_t>(result);
        if (client->response_sent < client->response_frame.size()) {
            std::string error;
            if (!submit_send_frame(client, error)) {
                Logger::error("io_uring resubmit send failed: " + error);
                close_client(client);
            }
            return;
        }

        client->response_frame.clear();
        client->response_sent = 0;
        std::string error;
        if (!submit_recv_length(client, error)) {
            Logger::error("io_uring submit next recv length failed: " + error);
            close_client(client);
        }
    }

    SocketServer server_;
    IoUringRing ring_;
    RequestProcessor processor_;
    std::atomic<bool> running_ {true};
};

}  // namespace

int run_server(int port, const std::string &root, const std::string &mode) {
    if (mode == "--io-uring") {
        IoUringServer server(root);
        return server.run(port);
    }
    if (mode == "--epoll") {
        EpollServer server(root);
        return server.run(port);
    }
    FlexQLServer server(root);
    return server.run(port);
}

}  // namespace flexql
