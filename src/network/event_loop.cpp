#include "network/event_loop.h"

#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <vector>

namespace flexql {

EventLoop::EventLoop() : fd_(-1) {}

EventLoop::~EventLoop() {
    close();
}

bool EventLoop::open(std::string &error_message) {
    close();
    fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (fd_ < 0) {
        error_message = std::strerror(errno);
        return false;
    }
    return true;
}

void EventLoop::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool EventLoop::add(int file_descriptor, std::uint32_t event_mask, std::string &error_message) {
    epoll_event registration {};
    registration.events = event_mask;
    registration.data.fd = file_descriptor;
    if (::epoll_ctl(fd_, EPOLL_CTL_ADD, file_descriptor, &registration) < 0) {
        error_message = std::strerror(errno);
        return false;
    }
    return true;
}

bool EventLoop::modify(
    int file_descriptor,
    std::uint32_t event_mask,
    std::string &error_message) {
    epoll_event registration {};
    registration.events = event_mask;
    registration.data.fd = file_descriptor;
    if (::epoll_ctl(fd_, EPOLL_CTL_MOD, file_descriptor, &registration) < 0) {
        error_message = std::strerror(errno);
        return false;
    }
    return true;
}

bool EventLoop::remove(int file_descriptor, std::string &error_message) {
    if (::epoll_ctl(fd_, EPOLL_CTL_DEL, file_descriptor, nullptr) < 0 &&
        errno != ENOENT &&
        errno != EBADF) {
        error_message = std::strerror(errno);
        return false;
    }
    return true;
}

int EventLoop::wait(
    std::vector<EventLoopEvent> &ready_events,
    int timeout_millis,
    std::string &error_message) {
    std::vector<epoll_event> raw_events(ready_events.size());
    const int ready_count = ::epoll_wait(
        fd_,
        raw_events.data(),
        static_cast<int>(raw_events.size()),
        timeout_millis);
    if (ready_count < 0) {
        if (errno == EINTR) {
            return 0;
        }

        error_message = std::strerror(errno);
        return -1;
    }

    for (int ready_index = 0; ready_index < ready_count; ++ready_index) {
        ready_events[static_cast<std::size_t>(ready_index)].fd =
            raw_events[static_cast<std::size_t>(ready_index)].data.fd;
        ready_events[static_cast<std::size_t>(ready_index)].events =
            raw_events[static_cast<std::size_t>(ready_index)].events;
    }

    return ready_count;
}

}  // namespace flexql
