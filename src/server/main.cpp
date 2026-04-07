#include <iostream>
#include <string>

namespace flexql {
int run_server(int port, const std::string &root, const std::string &mode);
}

int main(int argc, char **argv) {
    constexpr int kRequiredArgs = 2;
    constexpr int kOptionalArgs = 3;
    if (argc != kRequiredArgs && argc != kOptionalArgs) {
        std::cerr << "usage: " << argv[0] << " <port> [--io-uring|--epoll]\n";
        return 1;
    }

    std::string server_mode = "threaded";
    if (argc == kOptionalArgs) {
        server_mode = argv[2];
    }

    const int port_number = std::stoi(argv[1]);
    return flexql::run_server(port_number, ".", server_mode);
}
