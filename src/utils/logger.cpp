#include "utils/logger.h"

#include <iostream>
#include <mutex>

namespace flexql {
namespace {
std::mutex g_log_guard;
}

void Logger::info(const std::string &message_text) {
    std::lock_guard<std::mutex> guard(g_log_guard);
    std::cerr << "[INFO] " << message_text << '\n';
}

void Logger::error(const std::string &message_text) {
    std::lock_guard<std::mutex> guard(g_log_guard);
    std::cerr << "[ERROR] " << message_text << '\n';
}

}  // namespace flexql
