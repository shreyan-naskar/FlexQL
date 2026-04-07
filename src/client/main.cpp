#include <iostream>

#include <flexql.h>

namespace flexql {
int run_repl(FlexQL *db);
}

int main(int argument_count, char **argument_values) {
    constexpr int kExpectedArgs = 3;
    if (argument_count != kExpectedArgs) {
        std::cerr << "usage: " << argument_values[0] << " <host> <port>\n";
        return 1;
    }

    FlexQL *session = nullptr;
    const char *host_name = argument_values[1];
    const int port_number = std::stoi(argument_values[2]);
    if (flexql_open(host_name, port_number, &session) != 0) {
        std::cerr << "failed to connect to FlexQL server\n";
        return 1;
    }

    std::cout << "Connected to FlexQL server\n";
    const int exit_code = flexql::run_repl(session);
    flexql_close(session);
    return exit_code;
}
