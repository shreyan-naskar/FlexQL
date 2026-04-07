#pragma once

#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "common/types.h"

namespace flexql {

// Segmented LRU cache with two zones:
//   cold — probation segment; new entries land here, evicted first
//   hot  — protected segment; entries promoted here on second access
//
// Temporal locality: frequently reused query results survive longer
// than one-off results that happen to be recent.
class LruCache {
public:
    explicit LruCache(std::size_t capacity_limit = 256);

    bool get(const std::string &cache_key, QueryResult &cached_result);
    void put(
        const std::string &cache_key,
        const QueryResult &cached_result,
        const std::vector<std::string> &dependencies = {});
    void clear();
    void invalidate_table(const std::string &dependency_key);

private:
    using Entry = std::pair<std::string, QueryResult>;

    // Slot tracks which list an entry lives in and its iterator position.
    struct Slot {
        std::list<Entry>::iterator it;
        bool in_protected = false;
    };

    std::size_t probation_capacity_;   // capacity / 3
    std::size_t protected_capacity_;   // capacity * 2 / 3

    std::list<Entry> probation_;   // probation — new entries, evicted first
    std::list<Entry> protected_;   // protected — promoted on second access
    std::unordered_map<std::string, Slot> map_;
    std::unordered_map<std::string, std::unordered_set<std::string>> dependency_map_;
    mutable std::shared_mutex mutex_;
};

}  // namespace flexql
