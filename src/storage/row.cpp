#include "storage/row.h"

#include <cstdint>
#include <cstring>

namespace flexql {
namespace {

template <typename T>
void append_pod(std::string &target, const T &pod_value) {
    target.append(reinterpret_cast<const char *>(&pod_value), sizeof(T));
}

template <typename T>
bool read_pod(std::string_view source_bytes, std::size_t &cursor, T &pod_value) {
    if (cursor + sizeof(T) > source_bytes.size()) {
        return false;
    }

    std::memcpy(&pod_value, source_bytes.data() + cursor, sizeof(T));
    cursor += sizeof(T);
    return true;
}

}  // namespace

std::string serialize_row(const Row &row) {
    std::size_t payload_bytes = sizeof(std::int64_t) + sizeof(std::uint32_t);
    for (const auto &field_value : row.values) {
        payload_bytes += sizeof(std::uint32_t) + field_value.size();
    }

    std::string serialized;
    serialized.reserve(sizeof(std::uint32_t) + payload_bytes);

    append_pod(serialized, static_cast<std::uint32_t>(payload_bytes));
    append_pod(serialized, static_cast<std::int64_t>(row.expiration));
    append_pod(serialized, static_cast<std::uint32_t>(row.values.size()));
    for (const auto &field_value : row.values) {
        append_pod(serialized, static_cast<std::uint32_t>(field_value.size()));
        serialized.append(field_value.data(), field_value.size());
    }
    return serialized;
}

bool deserialize_row(
    const std::string &serialized_row,
    Row &row,
    std::string &failure_reason) {
    return deserialize_row(std::string_view(serialized_row), row, failure_reason);
}

bool deserialize_row(std::string_view payload, Row &row, std::string &failure_reason) {
    std::size_t cursor = 0;
    std::int64_t expiration_value = 0;
    if (!read_pod(payload, cursor, expiration_value)) {
        failure_reason = "corrupt row";
        return false;
    }

    std::uint32_t value_count = 0;
    if (!read_pod(payload, cursor, value_count)) {
        failure_reason = "corrupt row";
        return false;
    }

    row.expiration = static_cast<std::time_t>(expiration_value);
    row.values.clear();
    row.values.reserve(value_count);
    for (std::uint32_t value_index = 0; value_index < value_count; ++value_index) {
        std::uint32_t field_size = 0;
        if (!read_pod(payload, cursor, field_size) || cursor + field_size > payload.size()) {
            failure_reason = "corrupt row";
            return false;
        }
        row.values.emplace_back(payload.substr(cursor, field_size));
        cursor += field_size;
    }

    if (cursor != payload.size()) {
        failure_reason = "corrupt row";
        return false;
    }
    return true;
}

bool read_row_record_view(
    std::string_view mapped_data,
    std::size_t start_offset,
    RowView &row,
    std::size_t &next_offset,
    std::string &failure_reason) {
    std::size_t cursor = start_offset;
    std::uint32_t payload_size = 0;
    if (!read_pod(mapped_data, cursor, payload_size)) {
        failure_reason = "corrupt row";
        return false;
    }
    if (cursor + payload_size > mapped_data.size()) {
        failure_reason = "corrupt row";
        return false;
    }
    next_offset = cursor + payload_size;

    std::int64_t expiration_value = 0;
    if (!read_pod(mapped_data, cursor, expiration_value)) {
        failure_reason = "corrupt row";
        return false;
    }
    std::uint32_t column_count = 0;
    if (!read_pod(mapped_data, cursor, column_count)) {
        failure_reason = "corrupt row";
        return false;
    }

    row.expiration = static_cast<std::time_t>(expiration_value);
    row.values.clear();
    row.values.reserve(column_count);
    for (std::uint32_t column_index = 0; column_index < column_count; ++column_index) {
        std::uint32_t field_size = 0;
        if (!read_pod(mapped_data, cursor, field_size) ||
            cursor + field_size > mapped_data.size()) {
            failure_reason = "corrupt row";
            return false;
        }
        row.values.emplace_back(mapped_data.substr(cursor, field_size));
        cursor += field_size;
    }
    return true;
}

bool read_row_record(
    std::string_view mapped_data,
    std::size_t start_offset,
    Row &row,
    std::size_t &next_offset,
    std::string &failure_reason) {
    std::uint32_t payload_size = 0;
    std::size_t cursor = start_offset;
    if (!read_pod(mapped_data, cursor, payload_size)) {
        failure_reason = "corrupt row";
        return false;
    }
    if (cursor + payload_size > mapped_data.size()) {
        failure_reason = "corrupt row";
        return false;
    }

    next_offset = cursor + payload_size;
    return deserialize_row(mapped_data.substr(cursor, payload_size), row, failure_reason);
}

}  // namespace flexql
