#pragma once

#include <ctime>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"

namespace flexql {

// Zero-copy view into mmap memory — values are string_views into the mapped
// region and are valid only while the MmapReader that owns the memory is open.
struct RowView {
    std::vector<std::string_view> values;
    std::time_t expiration = 0;
};

std::string serialize_row(const Row &row);
bool deserialize_row(std::string_view payload, Row &row, std::string &error_message);
bool deserialize_row(const std::string &serialized_row, Row &row, std::string &error_message);
bool read_row_record(
    std::string_view data,
    std::size_t offset,
    Row &row,
    std::size_t &next_offset,
    std::string &error_message);
bool read_row_record_view(
    std::string_view data,
    std::size_t offset,
    RowView &row,
    std::size_t &next_offset,
    std::string &error_message);

// Convert a RowView to an owning Row (copies strings out of the mmap).
inline Row row_from_view(const RowView &row_view) {
    Row materialized_row;
    materialized_row.expiration = row_view.expiration;
    materialized_row.values.reserve(row_view.values.size());
    for (const auto &value_view : row_view.values) {
        materialized_row.values.emplace_back(value_view);
    }
    return materialized_row;
}

}  // namespace flexql
