#include "concurrency/lock_manager.h"

namespace flexql {

std::shared_mutex &LockManager::table_mutex(const std::string &resource_key) {
    std::lock_guard<std::mutex> guard(mutex_);

    const auto existing = table_locks_.find(resource_key);
    if (existing != table_locks_.end()) {
        return *existing->second;
    }

    auto [slot, was_inserted] = table_locks_.emplace(
        resource_key,
        std::make_shared<std::shared_mutex>());
    (void)was_inserted;
    return *slot->second;
}

}  // namespace flexql
