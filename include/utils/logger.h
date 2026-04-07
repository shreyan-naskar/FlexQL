#pragma once

#include <string>

namespace flexql {

class Logger {
public:
    static void info(const std::string &message_text);
    static void error(const std::string &message_text);
};

}  // namespace flexql
