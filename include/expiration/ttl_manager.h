#pragma once

#include <ctime>

namespace flexql {

class TtlManager {
public:
    explicit TtlManager(std::time_t default_ttl = 24 * 60 * 60);

    std::time_t compute_expiration() const;
    bool is_expired(std::time_t expiration_time) const;

private:
    std::time_t default_ttl_seconds_;
};

}  // namespace flexql
