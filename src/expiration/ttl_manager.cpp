#include "expiration/ttl_manager.h"

#include <ctime>

namespace flexql {

TtlManager::TtlManager(std::time_t default_ttl)
    : default_ttl_seconds_(default_ttl) {}

std::time_t TtlManager::compute_expiration() const {
    const std::time_t current_time = std::time(nullptr);
    return current_time + default_ttl_seconds_;
}

bool TtlManager::is_expired(std::time_t expiration_time) const {
    if (expiration_time == 0) {
        return false;
    }

    const std::time_t current_time = std::time(nullptr);
    return current_time > expiration_time;
}

}  // namespace flexql
