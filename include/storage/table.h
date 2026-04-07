#pragma once

#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "concurrency/lockfree_append.h"
#include "concurrency/lock_manager.h"
#include "index/btree_index.h"
#include "storage/mmap_reader.h"
#include "storage/row.h"
#include "storage/schema.h"
#include "utils/helpers.h"

namespace flexql {

class Table {
public:
    Table(
        const std::string &root_path,
        std::string database_label,
        std::string table_label,
        LockManager &lock_manager);
    ~Table();

    bool create(const std::vector<ColumnDef> &columns, std::string &error_message);
    bool drop(std::string &error_message);
    bool load(std::string &error_message);
    bool insert_row(
        const std::vector<std::string> &values,
        std::time_t expiration_time,
        std::string &error_message);
    bool insert_rows(
        const std::vector<std::vector<std::string>> &rows,
        std::time_t expiration_time,
        std::string &error_message);
    bool read_all(std::vector<Row> &rows_out, std::string &error_message);
    bool read_by_primary_key(
        const std::string &primary_key,
        Row &row_out,
        std::string &error_message);
    bool scan_rows(
        const std::function<bool(const Row &)> &visitor,
        std::string &error_message);
    bool scan_rows_matching(
        const Condition &condition,
        const std::function<bool(const Row &)> &visitor,
        std::string &error_message);

    // Template overloads: the visitor type is known at each call site so the
    // compiler can inline the lambda body directly — no std::function heap
    // allocation or virtual dispatch per row.
    template <typename Visitor>
    bool scan_rows_t(Visitor &&visitor, std::string &error_message) {
        {
            std::shared_lock<std::shared_mutex> read_lock(mutex());
            if (loaded_ && !reader_is_stale()) {
                return for_each_row_view(
                    reader_.view(),
                    std::forward<Visitor>(visitor),
                    error_message);
            }
        }
        {
            std::unique_lock<std::shared_mutex> write_lock(mutex());
            if (!ensure_loaded_locked(error_message)) {
                return false;
            }
            if (reader_is_stale() && !refresh_reader_locked(error_message)) {
                return false;
            }
        }
        std::shared_lock<std::shared_mutex> read_lock(mutex());
        return for_each_row_view(reader_.view(), std::forward<Visitor>(visitor), error_message);
    }

    // scan_rows_matching_t: visitor receives a RowView — no allocation for
    // non-matching rows.
    template <typename Visitor>
    bool scan_rows_matching_t(
        const Condition &condition,
        Visitor &&visitor,
        std::string &error_message) {
        if (!condition.enabled) {
            return scan_rows_t(std::forward<Visitor>(visitor), error_message);
        }
        std::string column_name = trim(condition.column);
        const auto dot_position = column_name.find('.');
        if (dot_position != std::string::npos) {
            column_name = column_name.substr(dot_position + 1);
        }
        const int column_index = schema_.column_index(column_name);
        if (column_index < 0) {
            error_message = "unknown WHERE column";
            return false;
        }
        std::string comparison_value = trim(condition.value);
        if (comparison_value.size() >= 2 &&
            comparison_value.front() == '\'' &&
            comparison_value.back() == '\'') {
            comparison_value = comparison_value.substr(1, comparison_value.size() - 2);
        }
        const DataType column_type =
            schema_.columns()[static_cast<std::size_t>(column_index)].type;
        return scan_rows_t(
            [&](const RowView &row_view) {
                if (column_index < static_cast<int>(row_view.values.size()) &&
                    compare_values(
                        std::string(row_view.values[static_cast<std::size_t>(column_index)]),
                        comparison_value,
                        column_type,
                        condition.op)) {
                    return visitor(row_view);
                }
                return true;
            },
            error_message);
    }
    const Schema &schema() const;
    std::vector<Row> filter_rows(const Condition &condition, std::string &error_message);
    BTreeIndex &index();
    std::shared_mutex &mutex();
    std::uintmax_t data_size(std::string &error_message) const;

private:
    // Hot scan loop: parse each record as a zero-copy RowView (string_views into
    // the mmap).  Visitors receive a RowView; call row_from_view() only for rows
    // that actually need to be materialised (e.g. after passing a WHERE filter).
    template <typename Visitor>
    static bool for_each_row_view(
        std::string_view mapped_bytes,
        Visitor &&visitor,
        std::string &error_message) {
        std::size_t cursor = 0;
        while (cursor < mapped_bytes.size()) {
            RowView row_view;
            std::size_t next_cursor = 0;
            if (!read_row_record_view(
                    mapped_bytes,
                    cursor,
                    row_view,
                    next_cursor,
                    error_message)) {
                return false;
            }
            if (!visitor(row_view)) {
                break;
            }
            cursor = next_cursor;
        }
        return true;
    }

    bool validate_row(
        const std::vector<std::string> &values,
        std::string &error_message) const;
    bool ensure_loaded_locked(std::string &error_message);
    bool open_append_file_locked(std::string &error_message);
    bool refresh_reader_locked(std::string &error_message);
    bool reader_is_stale() const;
    std::string database_root() const;
    std::string table_path() const;

    std::string root_;
    std::string database_name_;
    std::string name_;
    Schema schema_;
    BTreeIndex index_;
    LockManager &lock_manager_;
    bool loaded_ = false;
    LockFreeAppendFile append_file_;
    MmapReader reader_;
    mutable std::mutex write_state_mutex_;
    // Serialises concurrent delta log appends that happen outside write_state_mutex_.
    std::mutex index_flush_mutex_;
    std::unordered_set<std::string> inflight_primary_keys_;
};

}  // namespace flexql
