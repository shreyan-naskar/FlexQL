#include "cache/lru_cache.h"

#include <ctime>
#include <shared_mutex>

namespace flexql {

namespace {

void erase_dependency_links(
    const std::string &cache_key,
    std::unordered_map<std::string, std::unordered_set<std::string>> &dependency_links) {
    for (auto dependency_it = dependency_links.begin(); dependency_it != dependency_links.end();) {
        dependency_it->second.erase(cache_key);
        if (dependency_it->second.empty()) {
            dependency_it = dependency_links.erase(dependency_it);
        } else {
            ++dependency_it;
        }
    }
}

}  // namespace

LruCache::LruCache(std::size_t max_entries)
    : probation_capacity_(max_entries / 3 > 0 ? max_entries / 3 : 1),
      protected_capacity_(max_entries - (max_entries / 3 > 0 ? max_entries / 3 : 1)) {}

// get() uses a unique lock because a probation HIT splices entries between
// lists (mutation), so a shared lock is insufficient.
bool LruCache::get(const std::string &lookup_key, QueryResult &cached_result) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    const auto entry_it = map_.find(lookup_key);
    if (entry_it == map_.end()) {
        return false;
    }

    const std::time_t now = std::time(nullptr);
    if (entry_it->second.it->second.cache_expires_at != 0 &&
        now > entry_it->second.it->second.cache_expires_at) {
        // Expired — leave eviction to the next put().
        return false;
    }

    if (!entry_it->second.in_protected) {
        // PROBATION HIT → promote to front of protected (temporal locality).
        protected_.splice(protected_.begin(), probation_, entry_it->second.it);
        entry_it->second.in_protected = true;

        // If protected is now over capacity, demote its LRU tail to probation.
        if (protected_.size() > protected_capacity_) {
            auto demoted = std::prev(protected_.end());
            const std::string demoted_key = demoted->first;
            probation_.splice(probation_.begin(), protected_, demoted);
            map_[demoted_key].in_protected = false;
        }
    } else {
        // PROTECTED HIT → move to front (most recently used).
        protected_.splice(protected_.begin(), protected_, entry_it->second.it);
    }

    cached_result = entry_it->second.it->second;
    return true;
}

void LruCache::put(
    const std::string &entry_key,
    const QueryResult &cached_result,
    const std::vector<std::string> &dependency_keys) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    const auto existing = map_.find(entry_key);
    if (existing != map_.end()) {
        existing->second.it->second = cached_result;
        // Refresh position within whichever segment the entry already lives in.
        auto &seg = existing->second.in_protected ? protected_ : probation_;
        seg.splice(seg.begin(), seg, existing->second.it);
        erase_dependency_links(entry_key, dependency_map_);
        for (const auto &dep : dependency_keys) {
            dependency_map_[dep].insert(entry_key);
        }
        return;
    }

    // New entry → cold probation segment.
    probation_.push_front({entry_key, cached_result});
    map_[entry_key] = {probation_.begin(), false};
    for (const auto &dep : dependency_keys) {
        dependency_map_[dep].insert(entry_key);
    }

    // Evict LRU of probation when cold segment is over capacity.
    // Cold entries (never reused) die here; hot entries in protected are safe.
    if (probation_.size() > probation_capacity_) {
        auto oldest = std::prev(probation_.end());
        erase_dependency_links(oldest->first, dependency_map_);
        map_.erase(oldest->first);
        probation_.pop_back();
    }
}

void LruCache::clear() {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    map_.clear();
    probation_.clear();
    protected_.clear();
    dependency_map_.clear();
}

void LruCache::invalidate_table(const std::string &dependency_key) {
    std::unique_lock<std::shared_mutex> guard(mutex_);
    const auto dependency_it = dependency_map_.find(dependency_key);
    if (dependency_it == dependency_map_.end()) {
        return;
    }

    std::vector<std::string> affected_entries(
        dependency_it->second.begin(),
        dependency_it->second.end());
    dependency_map_.erase(dependency_it);
    for (const auto &entry_key : affected_entries) {
        const auto map_it = map_.find(entry_key);
        if (map_it == map_.end()) {
            continue;
        }

        erase_dependency_links(entry_key, dependency_map_);
        auto &seg = map_it->second.in_protected ? protected_ : probation_;
        seg.erase(map_it->second.it);
        map_.erase(map_it);
    }
}

}  // namespace flexql
