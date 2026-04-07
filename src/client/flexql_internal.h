#pragma once

#include <mutex>

#include "network/socket_client.h"

struct FlexQL {
    flexql::SocketClient socket_client;
    std::mutex state_mutex;
    bool is_open = false;
};
