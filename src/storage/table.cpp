#include "storage/table.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>

#include "query/vectorized_executor.h"
#include "storage/row.h"
#include "utils/helpers.h"

namespace flexql {

namespace {

template <typename Visitor>
bool for_each_mapped_row(std::string_view mapped_data, Visitor &&visitor, std::string &failure_reason) {
    std::size_t cursor = 0;
    while (cursor < mapped_data.size()) {
        Row decoded_row;
        std::size_t next_cursor = 0;
        if (!read_row_record(mapped_data, cursor, decoded_row, next_cursor, failure_reason)) {
            return false;
        }
        if (!visitor(decoded_row)) {
            break;
        }
        cursor = next_cursor;
    }
    return true;
}

}  // namespace

Table::Table(
    const std::string &root_path,
    std::string database_label,
    std::string table_label,
    LockManager &lock_manager)
    : root_(root_path),
      database_name_(std::move(database_label)),
      name_(std::move(table_label)),
      schema_(database_name_, name_),
      index_(database_name_, name_),
      lock_manager_(lock_manager) {}

Table::~Table() {
    std::unique_lock<std::shared_mutex> lock(mutex());
    reader_.close();
    append_file_.close();
    if (loaded_) {
        std::string error;
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        index_.compact(root_, error);  // full snapshot + bloom, safe at shutdown
    }
}

bool Table::create(const std::vector<ColumnDef> &columns, std::string &error) {
    std::unique_lock<std::shared_mutex> table_lock(mutex());
    if (std::filesystem::exists(table_path()) ||
        std::filesystem::exists(database_root() + "/tables/" + name_ + ".schema")) {
        error = "table already exists";
        return false;
    }
    std::filesystem::create_directories(database_root() + "/tables");
    std::ofstream table_stream(table_path(), std::ios::out | std::ios::trunc | std::ios::binary);
    if (!table_stream) {
        error = "failed to create table file";
        return false;
    }
    schema_.set_columns(columns);
    loaded_ = true;
    if (!schema_.save(root_, error)) {
        return false;
    }
    return index_.save(root_, error);
}

bool Table::drop(std::string &error) {
    std::unique_lock<std::shared_mutex> table_lock(mutex());
    reader_.close();
    append_file_.close();

    bool removed_any = false;
    std::error_code ec;
    removed_any |= std::filesystem::remove(table_path(), ec);
    ec.clear();
    removed_any |= std::filesystem::remove(database_root() + "/tables/" + name_ + ".schema", ec);
    ec.clear();
    removed_any |= std::filesystem::remove(database_root() + "/indexes/" + name_ + ".idx", ec);
    ec.clear();
    removed_any |= std::filesystem::remove(database_root() + "/indexes/" + name_ + ".idx.delta", ec);
    ec.clear();
    removed_any |= std::filesystem::remove(database_root() + "/indexes/" + name_ + ".idx.bloom", ec);

    if (!removed_any) {
        error = "table not found";
        return false;
    }

    loaded_ = false;
    schema_ = Schema(database_name_, name_);
    index_ = BTreeIndex(database_name_, name_);
    std::lock_guard<std::mutex> write_guard(write_state_mutex_);
    inflight_primary_keys_.clear();
    return true;
}

bool Table::load(std::string &error) {
    std::unique_lock<std::shared_mutex> table_lock(mutex());
    return ensure_loaded_locked(error);
}

bool Table::validate_row(const std::vector<std::string> &values, std::string &error) const {
    if (values.size() != schema_.columns().size()) {
        error = "column count mismatch";
        return false;
    }
    for (std::size_t column_index = 0; column_index < values.size(); ++column_index) {
        if (!validate_value_for_type(values[column_index], schema_.columns()[column_index].type)) {
            error = "type validation failed for column " + schema_.columns()[column_index].name;
            return false;
        }
    }
    return true;
}

bool Table::insert_row(const std::vector<std::string> &values, std::time_t expiration, std::string &error) {
    {
        std::shared_lock<std::shared_mutex> lock(mutex());
        if (!loaded_) {
            lock.unlock();
            std::unique_lock<std::shared_mutex> unique_lock(mutex());
            if (!ensure_loaded_locked(error)) {
                return false;
            }
        }
    }
    if (!validate_row(values, error)) {
        return false;
    }
    {
        std::unique_lock<std::shared_mutex> lock(mutex());
        if (!open_append_file_locked(error)) {
            return false;
        }
    }

    Row row;
    row.values.reserve(values.size());
    for (const auto &value : values) {
        std::string clean = trim(value);
        if (clean.size() >= 2 && clean.front() == '\'' && clean.back() == '\'') {
            clean = clean.substr(1, clean.size() - 2);
        }
        row.values.push_back(clean);
    }
    row.expiration = expiration;

    const int primary_key_index = schema_.primary_key_index();
    if (primary_key_index < 0 || primary_key_index >= static_cast<int>(row.values.size())) {
        error = "invalid primary key definition";
        return false;
    }
    const std::string primary_key_value = row.values[primary_key_index];
    const std::string record = serialize_row(row);
    std::uint64_t offset = 0;

    {
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        std::uint64_t existing_offset = 0;
        if (index_.get(primary_key_value, existing_offset) ||
            inflight_primary_keys_.find(primary_key_value) != inflight_primary_keys_.end()) {
            error = "duplicate primary key";
            return false;
        }
        offset = append_file_.reserve(record.size());
        inflight_primary_keys_.insert(primary_key_value);
    }

    if (!append_file_.write_at(offset, record, error)) {
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        inflight_primary_keys_.erase(primary_key_value);
        return false;
    }

    // Step 3: update index; drain the pending delta batch if a flush is due.
    // The actual file write happens OUTSIDE the mutex so other inserts are not
    // blocked for the duration of the disk I/O.
    std::vector<std::pair<std::string, std::uint64_t>> delta_batch;
    {
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        inflight_primary_keys_.erase(primary_key_value);
        index_.put(primary_key_value, offset);
        if (index_.needs_flush()) {
            index_.drain_delta(delta_batch);  // O(4096) copy, then clears pending
        }
    }

    if (!delta_batch.empty()) {
        // Serialise concurrent delta writers; each write is O(4096) entries ~115 KB.
        std::lock_guard<std::mutex> flush_guard(index_flush_mutex_);
        std::string idx_err;
        index_.write_delta_entries(root_, delta_batch, idx_err);
        // Non-fatal: index will be fully compacted at shutdown via compact().
    }
    return true;
}

bool Table::insert_rows(
    const std::vector<std::vector<std::string>> &rows,
    std::time_t expiration,
    std::string &error) {
    if (rows.empty()) {
        error = "no rows provided";
        return false;
    }

    {
        std::shared_lock<std::shared_mutex> lock(mutex());
        if (!loaded_) {
            lock.unlock();
            std::unique_lock<std::shared_mutex> unique_lock(mutex());
            if (!ensure_loaded_locked(error)) {
                return false;
            }
        }
    }
    for (const auto &values : rows) {
        if (!validate_row(values, error)) {
            return false;
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(mutex());
        if (!open_append_file_locked(error)) {
            return false;
        }
    }

    struct PreparedRow {
        std::string primary_key;
        std::string record;
        std::uint64_t offset = 0;
    };

    std::vector<PreparedRow> prepared_rows;
    prepared_rows.reserve(rows.size());
    std::size_t total_record_bytes = 0;
    const int primary_key_index = schema_.primary_key_index();
    if (primary_key_index < 0) {
        error = "invalid primary key definition";
        return false;
    }

    for (const auto &values : rows) {
        Row row;
        row.values.reserve(values.size());
        for (const auto &value : values) {
            std::string clean = trim(value);
            if (clean.size() >= 2 && clean.front() == '\'' && clean.back() == '\'') {
                clean = clean.substr(1, clean.size() - 2);
            }
            row.values.push_back(std::move(clean));
        }
        row.expiration = expiration;

        if (primary_key_index >= static_cast<int>(row.values.size())) {
            error = "invalid primary key definition";
            return false;
        }

        PreparedRow prepared;
        prepared.primary_key = row.values[static_cast<std::size_t>(primary_key_index)];
        prepared.record = serialize_row(row);
        total_record_bytes += prepared.record.size();
        prepared_rows.push_back(std::move(prepared));
    }

    std::vector<std::pair<std::string, std::uint64_t>> delta_batch;
    std::string combined_records;
    combined_records.reserve(total_record_bytes);

    {
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        std::unordered_set<std::string> batch_primary_keys;
        batch_primary_keys.reserve(prepared_rows.size());

        for (const auto &prepared : prepared_rows) {
            std::uint64_t existing_offset = 0;
            if (index_.get(prepared.primary_key, existing_offset) ||
                inflight_primary_keys_.find(prepared.primary_key) != inflight_primary_keys_.end() ||
                !batch_primary_keys.insert(prepared.primary_key).second) {
                error = "duplicate primary key";
                return false;
            }
        }

        std::uint64_t next_offset = append_file_.reserve(total_record_bytes);
        for (auto &prepared : prepared_rows) {
            prepared.offset = next_offset;
            next_offset += static_cast<std::uint64_t>(prepared.record.size());
            inflight_primary_keys_.insert(prepared.primary_key);
            combined_records += prepared.record;
        }
    }

    const std::uint64_t batch_offset = prepared_rows.front().offset;
    if (!append_file_.write_at(batch_offset, combined_records, error)) {
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        for (const auto &prepared : prepared_rows) {
            inflight_primary_keys_.erase(prepared.primary_key);
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> write_guard(write_state_mutex_);
        for (const auto &prepared : prepared_rows) {
            inflight_primary_keys_.erase(prepared.primary_key);
            index_.put(prepared.primary_key, prepared.offset);
        }
        if (index_.needs_flush()) {
            index_.drain_delta(delta_batch);
        }
    }

    if (!delta_batch.empty()) {
        std::lock_guard<std::mutex> flush_guard(index_flush_mutex_);
        std::string idx_err;
        index_.write_delta_entries(root_, delta_batch, idx_err);
    }

    return true;
}

bool Table::read_all(std::vector<Row> &rows, std::string &error) {
    // Fast path: reader is loaded and up-to-date
    {
        std::shared_lock<std::shared_mutex> read_lock(mutex());
        if (loaded_ && !reader_is_stale()) {
            rows.clear();
            return for_each_mapped_row(
                reader_.view(),
                [&](const Row &row) { rows.push_back(row); return true; },
                error);
        }
    }
    // Slow path: refresh under unique lock, then scan under shared lock
    {
        std::unique_lock<std::shared_mutex> refresh_lock(mutex());
        if (!ensure_loaded_locked(error)) return false;
        if (reader_is_stale() && !refresh_reader_locked(error)) return false;
    }
    std::shared_lock<std::shared_mutex> read_lock(mutex());
    rows.clear();
    return for_each_mapped_row(
        reader_.view(),
        [&](const Row &row) { rows.push_back(row); return true; },
        error);
}

bool Table::read_by_primary_key(const std::string &key, Row &row, std::string &error) {
    std::uint64_t offset = 0;
    // Fast path
    {
        std::shared_lock<std::shared_mutex> read_lock(mutex());
        if (loaded_ && !reader_is_stale()) {
            {
                std::lock_guard<std::mutex> wg(write_state_mutex_);
                if (!index_.get(key, offset)) { error = "row not found"; return false; }
            }
            if (offset >= reader_.size()) { error = "row not found"; return false; }
            std::size_t next = 0;
            return read_row_record(reader_.view(), static_cast<std::size_t>(offset), row, next, error);
        }
    }
    // Slow path: refresh then read
    {
        std::unique_lock<std::shared_mutex> refresh_lock(mutex());
        if (!ensure_loaded_locked(error)) return false;
        if (reader_is_stale() && !refresh_reader_locked(error)) return false;
    }
    {
        std::lock_guard<std::mutex> wg(write_state_mutex_);
        if (!index_.get(key, offset)) { error = "row not found"; return false; }
    }
    std::shared_lock<std::shared_mutex> read_lock(mutex());
    if (offset >= reader_.size()) { error = "row not found"; return false; }
    std::size_t next = 0;
    return read_row_record(reader_.view(), static_cast<std::size_t>(offset), row, next, error);
}

bool Table::scan_rows(const std::function<bool(const Row &)> &visitor, std::string &error) {
    // Fast path: reader is loaded and up-to-date
    {
        std::shared_lock<std::shared_mutex> lock(mutex());
        if (loaded_ && !reader_is_stale()) {
            return for_each_mapped_row(reader_.view(), visitor, error);
        }
    }
    // Slow path: refresh under unique lock, then scan under shared lock
    {
        std::unique_lock<std::shared_mutex> lock(mutex());
        if (!ensure_loaded_locked(error)) return false;
        if (reader_is_stale() && !refresh_reader_locked(error)) return false;
    }
    std::shared_lock<std::shared_mutex> lock(mutex());
    return for_each_mapped_row(reader_.view(), visitor, error);
}

bool Table::scan_rows_matching(const Condition &condition, const std::function<bool(const Row &)> &visitor, std::string &error) {
    if (!condition.enabled) {
        return scan_rows(visitor, error);
    }

    std::string column_name = trim(condition.column);
    const auto dot = column_name.find('.');
    if (dot != std::string::npos) {
        column_name = column_name.substr(dot + 1);
    }
    const int column_index = schema_.column_index(column_name);
    if (column_index < 0) {
        error = "unknown WHERE column";
        return false;
    }

    std::string needle = trim(condition.value);
    if (needle.size() >= 2 && needle.front() == '\'' && needle.back() == '\'') {
        needle = needle.substr(1, needle.size() - 2);
    }
    const DataType type = schema_.columns()[static_cast<std::size_t>(column_index)].type;

    return scan_rows(
        [&](const Row &row) {
            if (column_index < static_cast<int>(row.values.size()) &&
                compare_values(row.values[static_cast<std::size_t>(column_index)], needle, type, condition.op)) {
                return visitor(row);
            }
            return true;
        },
        error);
}

const Schema &Table::schema() const {
    return schema_;
}

std::vector<Row> Table::filter_rows(const Condition &condition, std::string &error) {
    std::vector<Row> rows;
    if (!read_all(rows, error)) {
        return {};
    }
    if (!condition.enabled) {
        return rows;
    }
    std::string column_name = trim(condition.column);
    const auto dot = column_name.find('.');
    if (dot != std::string::npos) {
        column_name = column_name.substr(dot + 1);
    }
    const int column_index = schema_.column_index(column_name);
    if (column_index < 0) {
        error = "unknown WHERE column";
        return {};
    }

    std::string needle = trim(condition.value);
    if (needle.size() >= 2 && needle.front() == '\'' && needle.back() == '\'') {
        needle = needle.substr(1, needle.size() - 2);
    }
    const DataType type = schema_.columns()[static_cast<std::size_t>(column_index)].type;

    std::vector<Row> filtered;
    for (const auto &row : rows) {
        if (column_index < static_cast<int>(row.values.size()) &&
            compare_values(row.values[static_cast<std::size_t>(column_index)], needle, type, condition.op)) {
            filtered.push_back(row);
        }
    }
    return filtered;
}

BTreeIndex &Table::index() {
    return index_;
}

std::shared_mutex &Table::mutex() {
    return lock_manager_.table_mutex(database_name_ + "." + name_);
}

std::uintmax_t Table::data_size(std::string &error) const {
    std::error_code ec;
    const auto size = std::filesystem::file_size(table_path(), ec);
    if (ec) {
        error = "table file not found";
        return 0;
    }
    return size;
}

std::string Table::database_root() const {
    return root_ + "/data/databases/" + database_name_;
}

std::string Table::table_path() const {
    return database_root() + "/tables/" + name_ + ".tbl";
}

bool Table::ensure_loaded_locked(std::string &error) {
    if (loaded_) {
        return true;
    }
    if (!schema_.load(root_, database_name_, name_, error)) {
        return false;
    }
    if (!index_.load(root_, error)) {
        return false;
    }
    loaded_ = true;
    // Open the persistent reader immediately so the first scan hits the fast path.
    if (reader_.empty()) {
        std::string open_err;
        reader_.open(table_path(), open_err);  // empty table is fine; ignore error
    }
    return true;
}

bool Table::open_append_file_locked(std::string &error) {
    if (append_file_.is_open()) {
        return true;
    }
    return append_file_.open(table_path(), error);
}

bool Table::reader_is_stale() const {
    if (reader_.empty()) return true;
    // If the append file is open its atomic offset tells us the true end-of-data.
    return append_file_.is_open() && append_file_.size() > reader_.size();
}

bool Table::refresh_reader_locked(std::string &error) {
    reader_.close();
    if (!reader_.open(table_path(), error)) {
        error = "table file not found";
        return false;
    }
    return true;
}

}  // namespace flexql
