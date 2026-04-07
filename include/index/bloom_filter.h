#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flexql {

class BloomFilter {
public:
    BloomFilter(std::size_t bit_count_limit = 8192, std::size_t hash_count_limit = 3);

    void clear();
    void reset(std::size_t bit_count_limit, std::size_t hash_count_limit);
    void add(const std::string &entry_key);
    bool might_contain(const std::string &entry_key) const;

    bool save(const std::string &file_path, std::string &error_message) const;
    bool load(const std::string &file_path, std::string &error_message);

private:
    std::uint64_t hash_value(const std::string &entry_key, std::uint64_t seed_value) const;
    std::size_t bit_count_;
    std::size_t hash_count_;
    std::vector<std::uint8_t> bits_;
};

}  // namespace flexql
