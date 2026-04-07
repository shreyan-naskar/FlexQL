#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace flexql {

struct EventLoopEvent {
    int fd = -1;
    std::uint32_t events = 0;
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    bool open(std::string &error);
    void close();

    bool add(int file_descriptor, std::uint32_t event_mask, std::string &error_message);
    bool modify(int file_descriptor, std::uint32_t event_mask, std::string &error_message);
    bool remove(int file_descriptor, std::string &error_message);
    int wait(
        std::vector<EventLoopEvent> &ready_events,
        int timeout_millis,
        std::string &error_message);

private:
    int fd_;
};

}  // namespace flexql
