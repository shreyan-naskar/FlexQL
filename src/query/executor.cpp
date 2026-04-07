#include "query/executor.h"

#include <algorithm>
#include <filesystem>
#include <memory_resource>
#include <unordered_map>

#include "concurrency/lock_manager.h"
#include "memory/arena_allocator.h"
#include "query/vectorized_executor.h"
#include "utils/helpers.h"
#include "utils/logger.h"

namespace flexql {
namespace {

LockManager &global_lock_manager() {
    static LockManager shared_manager;
    return shared_manager;
}

std::string database_root(const std::string &root_path, const std::string &database_name) {
    return root_path + "/data/databases/" + database_name;
}

void ensure_database_layout(const std::string &root_path, const std::string &database_name) {
    const std::string base_path = database_root(root_path, database_name);
    std::filesystem::create_directories(base_path + "/tables");
    std::filesystem::create_directories(base_path + "/indexes");
    std::filesystem::create_directories(base_path + "/wal");
}

bool database_exists(const std::string &root_path, const std::string &database_name) {
    return std::filesystem::exists(database_root(root_path, database_name));
}

std::string resolve_database_name(const Query &query, const std::string &active_database) {
    if (!query.database.empty()) {
        return query.database;
    }
    return active_database.empty() ? "default" : active_database;
}

std::vector<std::string> build_projection_columns(
    const Schema &schema,
    const std::vector<std::string> &requested_columns) {
    if (requested_columns.size() == 1 && requested_columns[0] == "*") {
        std::vector<std::string> projected_columns;
        for (const auto &column : schema.columns()) {
            projected_columns.push_back(column.name);
        }
        return projected_columns;
    }
    return requested_columns;
}

std::string strip_qualifier(const std::string &value) {
    const auto dot = value.find('.');
    if (dot == std::string::npos) {
        return value;
    }
    return value.substr(dot + 1);
}

std::string build_simple_select_cache_key(const Query &query) {
    return normalize_sql(
        "simple:" + query.database + ":" + query.table + ":" + join(query.select_columns, ",") + ":" +
        (query.where.enabled ? query.where.column + ":" + std::to_string(static_cast<int>(query.where.op)) + ":" + query.where.value : ""));
}

std::string build_join_select_cache_key(const Query &query) {
    std::string key = "join:" + query.database + ":" + query.table;
    for (const auto &join_spec : query.joins) {
        key += ":" + join_spec.right_table + ":" + join_spec.left_column + "=" + join_spec.right_column;
    }
    key += ":" + join(query.select_columns, ",") + ":";
    if (query.where.enabled) {
        key += query.where.column + ":" +
            std::to_string(static_cast<int>(query.where.op)) + ":" + query.where.value;
    }
    return normalize_sql(key);
}

std::time_t combine_cache_expiration(std::time_t current, std::time_t candidate) {
    if (candidate == 0) {
        return current;
    }
    if (current == 0 || candidate < current) {
        return candidate;
    }
    return current;
}

struct JoinColumnRef {
    int index = -1;
    DataType type = DataType::Varchar;
};

struct JoinedColumnMeta {
    std::string table_name;
    std::string column_name;
    DataType type = DataType::Varchar;
};

struct JoinedRowData {
    std::vector<std::string> values;
    std::time_t expiration = 0;
};

struct TableColumnRef {
    int index = -1;
    DataType type = DataType::Varchar;
};

bool validate_simple_projection(const Schema &schema, const std::vector<std::string> &requested, std::string &error) {
    if (requested.size() == 1 && requested[0] == "*") {
        return true;
    }
    for (const auto &column : requested) {
        const int index = schema.column_index(strip_qualifier(column));
        if (index < 0) {
            error = "unknown SELECT column: " + column;
            return false;
        }
    }
    return true;
}

std::vector<std::string> project_row(const Schema &schema, const Row &row, const std::vector<std::string> &requested) {
    if (requested.size() == 1 && requested[0] == "*") {
        return row.values;
    }
    std::vector<std::string> out;
    out.reserve(requested.size());
    for (const auto &column : requested) {
        const int index = schema.column_index(strip_qualifier(column));
        out.push_back(row.values[static_cast<std::size_t>(index)]);
    }
    return out;
}

bool resolve_joined_column_ref(
    const std::vector<JoinedColumnMeta> &joined_columns,
    const std::string &raw_column,
    JoinColumnRef &ref,
    std::string &error) {
    const auto dot = raw_column.find('.');
    if (dot != std::string::npos) {
        const std::string table_name = to_upper(trim(raw_column.substr(0, dot)));
        const std::string column_name = trim(raw_column.substr(dot + 1));
        for (std::size_t column_index = 0; column_index < joined_columns.size(); ++column_index) {
            const auto &meta = joined_columns[column_index];
            if (to_upper(meta.table_name) == table_name &&
                to_upper(meta.column_name) == to_upper(column_name)) {
                ref = {static_cast<int>(column_index), meta.type};
                return true;
            }
        }
        error = "unknown column: " + raw_column;
        return false;
    }

    int resolved_index = -1;
    for (std::size_t column_index = 0; column_index < joined_columns.size(); ++column_index) {
        if (to_upper(joined_columns[column_index].column_name) != to_upper(raw_column)) {
            continue;
        }
        if (resolved_index >= 0) {
            error = "ambiguous column: " + raw_column;
            return false;
        }
        resolved_index = static_cast<int>(column_index);
    }
    if (resolved_index < 0) {
        error = "unknown column: " + raw_column;
        return false;
    }

    ref = {
        resolved_index,
        joined_columns[static_cast<std::size_t>(resolved_index)].type};
    return true;
}

bool resolve_table_column_ref(
    const Schema &schema,
    const std::string &raw_column,
    TableColumnRef &ref,
    std::string &error) {
    std::string column_name = trim(raw_column);
    const auto dot = raw_column.find('.');
    if (dot != std::string::npos) {
        const std::string table_name = to_upper(trim(raw_column.substr(0, dot)));
        if (table_name != to_upper(schema.table_name())) {
            error = "unknown table in column reference: " + raw_column;
            return false;
        }
        column_name = trim(raw_column.substr(dot + 1));
    }

    const int column_index = schema.column_index(column_name);
    if (column_index < 0) {
        error = "unknown column: " + raw_column;
        return false;
    }

    ref = {
        column_index,
        schema.columns()[static_cast<std::size_t>(column_index)].type};
    return true;
}

void append_joined_columns(
    const Schema &schema,
    std::vector<JoinedColumnMeta> &joined_columns,
    std::vector<std::string> *result_columns = nullptr) {
    for (const auto &column : schema.columns()) {
        joined_columns.push_back({schema.table_name(), column.name, column.type});
        if (result_columns != nullptr) {
            result_columns->push_back(schema.table_name() + "." + column.name);
        }
    }
}

std::string normalize_literal_value(const std::string &raw_value) {
    std::string normalized_value = trim(raw_value);
    if (normalized_value.size() >= 2 &&
        normalized_value.front() == '\'' &&
        normalized_value.back() == '\'') {
        normalized_value = normalized_value.substr(1, normalized_value.size() - 2);
    }
    return normalized_value;
}

std::string table_dependency_key(const std::string &database_name, const std::string &table_name) {
    return database_name + "." + table_name;
}

std::vector<std::string> collect_join_dependencies(const Query &query) {
    std::vector<std::string> dependencies = {
        table_dependency_key(query.database, query.table)};
    for (const auto &join_spec : query.joins) {
        dependencies.push_back(table_dependency_key(query.database, join_spec.right_table));
    }
    return dependencies;
}

void log_cache_event(const std::string &status, const std::string &cache_key) {
    Logger::info("CACHE " + status + ": " + cache_key);
}

// Build the page-cache key for a given table page.
std::string page_cache_key(
    const std::string &database_name,
    const std::string &table_name,
    int page_id) {
    return database_name + "." + table_name + ":" + std::to_string(page_id);
}

// Test whether a single row satisfies the query's WHERE clause.
// Returns true when the row should be included (no WHERE = always true).
bool where_matches(const Schema &schema, const Row &row, const Condition &where) {
    if (!where.enabled) {
        return true;
    }
    const int col_idx = schema.column_index(strip_qualifier(where.column));
    if (col_idx < 0 || col_idx >= static_cast<int>(row.values.size())) {
        return false;
    }
    std::string needle = trim(where.value);
    if (needle.size() >= 2 && needle.front() == '\'' && needle.back() == '\'') {
        needle = needle.substr(1, needle.size() - 2);
    }
    const DataType dtype = schema.columns()[static_cast<std::size_t>(col_idx)].type;
    return compare_values(row.values[static_cast<std::size_t>(col_idx)], needle, dtype, where.op);
}

}  // namespace

Executor::Executor(const std::string &root_path, std::shared_ptr<LruCache> cache)
    : data_root_(root_path), cache_(std::move(cache)) {
    ensure_database_layout(data_root_, "default");
}

// Load all rows for a table into the page cache if not already present.
// On HIT  → logs "CACHE HIT [page]"  and returns immediately (no disk I/O).
// On MISS → logs "CACHE MISS [page]", reads from disk, splits into 64-row pages.
bool Executor::ensure_pages_loaded(
    const std::string &database_name,
    const std::string &table_name,
    Table &table,
    std::string &error) {
    const std::string p0 = page_cache_key(database_name, table_name, 0);
    {
        std::lock_guard<std::mutex> pg(page_cache_mutex_);
        if (row_page_cache_.count(p0)) {
            log_cache_event("HIT [page]", database_name + "." + table_name);
            return true;
        }
    }

    log_cache_event("MISS [page]", database_name + "." + table_name);

    std::vector<Row> all_rows;
    if (!table.read_all(all_rows, error)) {
        return false;
    }

    std::lock_guard<std::mutex> pg(page_cache_mutex_);
    if (all_rows.empty()) {
        // Store an empty sentinel page so subsequent calls see a HIT.
        row_page_cache_[p0] = {};
        return true;
    }
    for (int i = 0; i < static_cast<int>(all_rows.size()); i += kPageSize) {
        const int pid = i / kPageSize;
        std::vector<Row> page;
        page.reserve(kPageSize);
        const int end = std::min(static_cast<int>(all_rows.size()), i + kPageSize);
        for (int j = i; j < end; ++j) {
            page.push_back(std::move(all_rows[static_cast<std::size_t>(j)]));
        }
        row_page_cache_[page_cache_key(database_name, table_name, pid)] = std::move(page);
    }
    return true;
}

// Reconstruct a flat row vector from the page cache (used by JOIN select).
void Executor::collect_page_rows(
    const std::string &database_name,
    const std::string &table_name,
    std::vector<Row> &out) const {
    std::lock_guard<std::mutex> pg(page_cache_mutex_);
    for (int pid = 0; ; ++pid) {
        const auto it = row_page_cache_.find(page_cache_key(database_name, table_name, pid));
        if (it == row_page_cache_.end()) {
            break;
        }
        out.insert(out.end(), it->second.begin(), it->second.end());
    }
}

std::shared_ptr<Table> Executor::get_table(
    const std::string &database_name,
    const std::string &table_name,
    std::string &error) {
    const std::string cache_key = database_name + "." + table_name;
    {
        std::lock_guard<std::mutex> guard(table_cache_mutex_);
        const auto cached_table = table_cache_.find(cache_key);
        if (cached_table != table_cache_.end()) {
            return cached_table->second;
        }
    }

    auto loaded_table = std::make_shared<Table>(
        data_root_,
        database_name,
        table_name,
        global_lock_manager());
    if (!loaded_table->load(error)) {
        return nullptr;
    }

    std::lock_guard<std::mutex> guard(table_cache_mutex_);
    auto [slot, inserted] = table_cache_.emplace(cache_key, loaded_table);
    return inserted ? loaded_table : slot->second;
}

QueryResult Executor::execute_sql(const std::string &sql, std::string &current_database) {
    // Only SELECT queries benefit from caching. For INSERT/CREATE/DROP/etc. skip the
    // O(sql-length) cache-key construction entirely — building and hashing a 100 KB+
    // INSERT string per batch nullifies any throughput gain from larger batch sizes.
    // Quick O(1) prefix check: skip leading whitespace, then compare first 6 chars.
    auto is_select_query = [](const std::string &s) -> bool {
        std::size_t i = 0;
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) { ++i; }
        if (s.size() - i < 6) { return false; }
        return (s[i]=='S'||s[i]=='s') && (s[i+1]=='E'||s[i+1]=='e') &&
               (s[i+2]=='L'||s[i+2]=='l') && (s[i+3]=='E'||s[i+3]=='e') &&
               (s[i+4]=='C'||s[i+4]=='c') && (s[i+5]=='T'||s[i+5]=='t');
    };

    // Pre-parse cache check: avoids regex parsing entirely on repeated SELECTs.
    std::string raw_cache_key;
    if (cache_ && is_select_query(sql)) {
        raw_cache_key = normalize_sql(
            (current_database.empty() ? "default" : current_database) + "::" + sql);
        QueryResult cached_result;
        if (cache_->get(raw_cache_key, cached_result)) {
            log_cache_event("HIT [raw]", raw_cache_key);
            return cached_result;
        }
        log_cache_event("MISS [raw]", raw_cache_key);
    }

    Query query;
    std::string error;
    if (!parser_.parse(sql, query, error)) {
        return {false, error, {}, {}};
    }
    if (query.type != QueryType::CreateDatabase && query.type != QueryType::ShowDatabases) {
        query.database = resolve_database_name(query, current_database);
    }
    QueryPlan plan {query};
    QueryResult result = execute(plan);
    if (result.success && query.type == QueryType::UseDatabase) {
        current_database = query.database;
    }
    // Cache successful SELECTs under the raw key so the next identical call
    // skips parsing.  Dependencies mirror those stored by execute_simple/join_select.
    if (result.success && cache_ && query.type == QueryType::Select && !raw_cache_key.empty()) {
        cache_->put(raw_cache_key, result, collect_join_dependencies(query));
    }
    return result;
}

QueryResult Executor::execute_batch_insert(const std::vector<std::string> &sql_batch, std::string &current_database) {
    int inserted = 0;
    for (std::size_t i = 0; i < sql_batch.size(); ++i) {
        QueryResult result = execute_sql(sql_batch[i], current_database);
        if (!result.success) {
            return {false, "batch failed at statement " + std::to_string(i + 1) + ": " + result.message, {}, {}};
        }
        ++inserted;
    }
    return {true, "OK, " + std::to_string(inserted) + " rows inserted.", {}, {}};
}

QueryResult Executor::execute(const QueryPlan &plan) {
    switch (plan.query.type) {
        case QueryType::CreateDatabase:
            return execute_create_database(plan.query);
        case QueryType::UseDatabase:
            return execute_use_database(plan.query);
        case QueryType::CreateTable:
            return execute_create(plan.query);
        case QueryType::DropTable:
            return execute_drop_table(plan.query);
        case QueryType::Insert:
            return execute_insert(plan.query);
        case QueryType::Select:
            return execute_select(plan.query);
        case QueryType::ShowDatabases:
            return execute_show_databases();
        case QueryType::ShowTables:
            return execute_show_tables(plan.query);
        default:
            return {false, "unsupported query type", {}, {}};
    }
}

QueryResult Executor::execute_create_database(const Query &query) {
    if (database_exists(data_root_, query.database)) {
        if (query.if_not_exists) {
            return {true, "OK, database already existed.", {}, {}};
        }
        return {false, "database already exists", {}, {}};
    }
    ensure_database_layout(data_root_, query.database);
    if (cache_) {
        cache_->clear();
    }
    return {true, "OK, 1 database created.", {}, {}};
}

QueryResult Executor::execute_use_database(const Query &query) {
    if (!database_exists(data_root_, query.database)) {
        return {false, "database not found", {}, {}};
    }
    return {true, "Database changed to '" + query.database + "'.", {}, {}};
}

QueryResult Executor::execute_create(const Query &query) {
    if (!database_exists(data_root_, query.database)) {
        return {false, "database not found", {}, {}};
    }
    auto table = std::make_shared<Table>(data_root_, query.database, query.table, global_lock_manager());
    std::string error;
    if (!table->create(query.column_defs, error)) {
        if (query.if_not_exists && error == "table already exists") {
            return {true, "OK, table already existed.", {}, {}};
        }
        return {false, error, {}, {}};
    }
    {
        std::lock_guard<std::mutex> lock(table_cache_mutex_);
        table_cache_[query.database + "." + query.table] = table;
    }
    invalidate_table_cache(query.database, query.table);
    return {true, "OK, table created.", {}, {}};
}

QueryResult Executor::execute_drop_table(const Query &query) {
    if (!database_exists(data_root_, query.database)) {
        if (query.if_exists) {
            return {true, "OK, table did not exist.", {}, {}};
        }
        return {false, "database not found", {}, {}};
    }

    std::string error;
    auto table = get_table(query.database, query.table, error);
    if (!table) {
        if (query.if_exists) {
            return {true, "OK, table did not exist.", {}, {}};
        }
        return {false, error, {}, {}};
    }
    if (!table->drop(error)) {
        if (query.if_exists && error == "table not found") {
            return {true, "OK, table did not exist.", {}, {}};
        }
        return {false, error, {}, {}};
    }
    {
        std::lock_guard<std::mutex> lock(table_cache_mutex_);
        table_cache_.erase(query.database + "." + query.table);
    }
    invalidate_table_cache(query.database, query.table);
    return {true, "OK, table dropped.", {}, {}};
}

QueryResult Executor::execute_insert(const Query &query) {
    if (!database_exists(data_root_, query.database)) {
        return {false, "database not found", {}, {}};
    }
    std::string error;
    auto table = get_table(query.database, query.table, error);
    if (!table) {
        return {false, error, {}, {}};
    }
    const std::time_t expiration = query.insert_expiration != 0 ? query.insert_expiration : ttl_manager_.compute_expiration();

    if (!query.insert_multi_values.empty()) {
        if (!table->insert_rows(query.insert_multi_values, expiration, error)) {
            return {false, error, {}, {}};
        }
        invalidate_table_cache(query.database, query.table);
        return {true, "OK, " + std::to_string(query.insert_multi_values.size()) + " rows inserted.", {}, {}};
    }

    if (!table->insert_row(query.insert_values, expiration, error)) {
        return {false, error, {}, {}};
    }
    invalidate_table_cache(query.database, query.table);
    return {true, "OK, 1 row inserted.", {}, {}};
}

QueryResult Executor::execute_select(const Query &query) {
    if (!database_exists(data_root_, query.database)) {
        return {false, "database not found", {}, {}};
    }
    std::string error;
    auto left = get_table(query.database, query.table, error);
    if (!left) {
        return {false, error, {}, {}};
    }

    if (!query.joins.empty()) {
        return execute_join_select(*left, query, build_join_select_cache_key(query));
    }

    return execute_simple_select(*left, query, build_simple_select_cache_key(query));
}

QueryResult Executor::execute_show_databases() {
    QueryResult result;
    result.success = true;
    result.message = "ok";
    result.columns = {"DATABASE"};

    const std::filesystem::path root = data_root_ + "/data/databases";
    if (std::filesystem::exists(root)) {
        for (const auto &entry : std::filesystem::directory_iterator(root)) {
            if (entry.is_directory()) {
                result.rows.push_back({entry.path().filename().string()});
            }
        }
    }
    std::sort(result.rows.begin(), result.rows.end());
    return result;
}

QueryResult Executor::execute_show_tables(const Query &query) {
    const std::filesystem::path tables_root = database_root(data_root_, query.database) + "/tables";
    if (!std::filesystem::exists(tables_root)) {
        return {false, "database not found", {}, {}};
    }

    QueryResult result;
    result.success = true;
    result.message = "ok";
    result.columns = {"TABLE"};
    for (const auto &entry : std::filesystem::directory_iterator(tables_root)) {
        if (entry.is_regular_file() && entry.path().extension() == ".schema") {
            result.rows.push_back({entry.path().stem().string()});
        }
    }
    std::sort(result.rows.begin(), result.rows.end());
    return result;
}

QueryResult Executor::execute_simple_select(Table &table, const Query &query, const std::string &normalized_sql) {
    QueryResult cached;
    if (cache_ && cache_->get(normalized_sql, cached)) {
        log_cache_event("HIT [select]", normalized_sql);
        return cached;
    }
    if (cache_) {
        log_cache_event("MISS [select]", normalized_sql);
    }

    std::string error;
    if (!validate_simple_projection(table.schema(), query.select_columns, error)) {
        return {false, error, {}, {}, 0};
    }
    if (query.where.enabled && table.schema().column_index(strip_qualifier(query.where.column)) < 0) {
        return {false, "unknown WHERE column", {}, {}, 0};
    }

    const std::string where_column = strip_qualifier(query.where.column);
    QueryResult result;
    result.success = true;
    result.message = "ok";
    result.columns = build_projection_columns(table.schema(), query.select_columns);

    if (query.where.enabled && query.where.op == ComparisonOp::Equal &&
        table.schema().column_index(where_column) == table.schema().primary_key_index()) {
        Row row;
        std::string key = trim(query.where.value);
        if (key.size() >= 2 && key.front() == '\'' && key.back() == '\'') {
            key = key.substr(1, key.size() - 2);
        }
        if (table.read_by_primary_key(key, row, error)) {
            if (!ttl_manager_.is_expired(row.expiration)) {
                result.rows.push_back(project_row(table.schema(), row, query.select_columns));
                result.cache_expires_at = combine_cache_expiration(result.cache_expires_at, row.expiration);
            }
        } else if (error != "row not found") {
            return {false, error, {}, {}};
        }
    } else {
        // Spatial locality: load the table into 64-row pages once, then serve
        // all subsequent queries for this table from the in-memory page cache.
        if (!ensure_pages_loaded(query.database, query.table, table, error)) {
            return {false, error, {}, {}};
        }
        for (int pid = 0; ; ++pid) {
            std::vector<Row> page_snapshot;
            {
                std::lock_guard<std::mutex> pg(page_cache_mutex_);
                const auto pit = row_page_cache_.find(
                    page_cache_key(query.database, query.table, pid));
                if (pit == row_page_cache_.end()) {
                    break;
                }
                page_snapshot = pit->second;
            }
            for (const auto &row : page_snapshot) {
                if (ttl_manager_.is_expired(row.expiration)) {
                    continue;
                }
                if (!where_matches(table.schema(), row, query.where)) {
                    continue;
                }
                result.rows.push_back(project_row(table.schema(), row, query.select_columns));
                result.cache_expires_at = combine_cache_expiration(
                    result.cache_expires_at, row.expiration);
            }
        }
    }
    if (cache_) {
        cache_->put(normalized_sql, result, {table_dependency_key(query.database, query.table)});
    }
    return result;
}

QueryResult Executor::execute_join_select(
    Table &left,
    const Query &query,
    const std::string &normalized_sql) {
    QueryResult cached;
    if (cache_ && cache_->get(normalized_sql, cached)) {
        log_cache_event("HIT [join]", normalized_sql);
        return cached;
    }
    if (cache_) {
        log_cache_event("MISS [join]", normalized_sql);
    }

    std::string error;
    std::vector<JoinedColumnMeta> joined_columns;
    QueryResult result;
    result.success = true;
    result.message = "ok";
    append_joined_columns(
        left.schema(),
        joined_columns,
        query.select_columns.size() == 1 && query.select_columns[0] == "*" ? &result.columns : nullptr);

    // Load left table via page cache (spatial locality).
    if (!ensure_pages_loaded(query.database, query.table, left, error)) {
        return {false, error, {}, {}};
    }
    std::vector<Row> left_rows;
    collect_page_rows(query.database, query.table, left_rows);

    std::vector<JoinedRowData> joined_rows;
    joined_rows.reserve(left_rows.size());
    for (const auto &row : left_rows) {
        if (ttl_manager_.is_expired(row.expiration)) {
            continue;
        }
        joined_rows.push_back({row.values, row.expiration});
    }

    for (const auto &join_spec : query.joins) {
        auto right_table = get_table(query.database, join_spec.right_table, error);
        if (!right_table) {
            return {false, error, {}, {}};
        }

        TableColumnRef right_join_ref;
        if (!resolve_table_column_ref(
                right_table->schema(),
                join_spec.right_column,
                right_join_ref,
                error)) {
            return {false, error, {}, {}};
        }
        JoinColumnRef left_join_ref;
        if (!resolve_joined_column_ref(joined_columns, join_spec.left_column, left_join_ref, error)) {
            return {false, error, {}, {}};
        }
        // Load right table via page cache (spatial locality).
        if (!ensure_pages_loaded(query.database, join_spec.right_table, *right_table, error)) {
            return {false, error, {}, {}};
        }
        std::vector<Row> right_rows;
        collect_page_rows(query.database, join_spec.right_table, right_rows);

        std::unordered_multimap<std::string, const Row *> hash_index;
        hash_index.reserve(right_rows.size());
        for (const auto &row : right_rows) {
            if (ttl_manager_.is_expired(row.expiration)) {
                continue;
            }
            hash_index.emplace(
                row.values[static_cast<std::size_t>(right_join_ref.index)],
                &row);
        }

        std::vector<JoinedRowData> next_joined_rows;
        for (const auto &current_row : joined_rows) {
            const auto range = hash_index.equal_range(
                current_row.values[static_cast<std::size_t>(left_join_ref.index)]);
            for (auto it = range.first; it != range.second; ++it) {
                if (!vectorized_equals(
                        current_row.values[static_cast<std::size_t>(left_join_ref.index)],
                        it->second->values[static_cast<std::size_t>(right_join_ref.index)])) {
                    continue;
                }

                JoinedRowData combined_row;
                combined_row.values = current_row.values;
                combined_row.values.insert(
                    combined_row.values.end(),
                    it->second->values.begin(),
                    it->second->values.end());
                combined_row.expiration = combine_cache_expiration(
                    current_row.expiration,
                    it->second->expiration);
                next_joined_rows.push_back(std::move(combined_row));
            }
        }

        append_joined_columns(
            right_table->schema(),
            joined_columns,
            query.select_columns.size() == 1 && query.select_columns[0] == "*" ? &result.columns : nullptr);
        joined_rows = std::move(next_joined_rows);
    }

    std::vector<JoinColumnRef> projected_columns;
    if (!(query.select_columns.size() == 1 && query.select_columns[0] == "*")) {
        result.columns = query.select_columns;
        projected_columns.reserve(query.select_columns.size());
        for (const auto &column : query.select_columns) {
            JoinColumnRef ref;
            if (!resolve_joined_column_ref(joined_columns, column, ref, error)) {
                return {false, error, {}, {}, 0};
            }
            projected_columns.push_back(ref);
        }
    }

    JoinColumnRef where_ref;
    if (query.where.enabled &&
        !resolve_joined_column_ref(joined_columns, query.where.column, where_ref, error)) {
        return {false, error, {}, {}, 0};
    }

    const std::string where_value = normalize_literal_value(query.where.value);
    for (const auto &joined_row : joined_rows) {
        if (query.where.enabled &&
            !compare_values(
                joined_row.values[static_cast<std::size_t>(where_ref.index)],
                where_value,
                where_ref.type,
                query.where.op)) {
            continue;
        }
        result.cache_expires_at = combine_cache_expiration(
            result.cache_expires_at,
            joined_row.expiration);
        if (query.select_columns.size() == 1 && query.select_columns[0] == "*") {
            result.rows.push_back(joined_row.values);
            continue;
        }
        std::vector<std::string> projected_values;
        projected_values.reserve(projected_columns.size());
        for (const auto &ref : projected_columns) {
            projected_values.push_back(
                joined_row.values[static_cast<std::size_t>(ref.index)]);
        }
        result.rows.push_back(std::move(projected_values));
    }

    if (cache_) {
        cache_->put(normalized_sql, result, collect_join_dependencies(query));
    }
    return result;
}

void Executor::invalidate_table_cache(const std::string &database_name, const std::string &table_name) {
    const std::string dep_key = table_dependency_key(database_name, table_name);

    if (cache_) {
        log_cache_event("INVALIDATE [query]", dep_key);
        cache_->invalidate_table(dep_key);
    }

    // Evict all pages for this table so the next read re-loads from disk.
    // Fast path: during bulk-insert workloads the page cache is empty — skip the scan.
    {
        std::lock_guard<std::mutex> pg(page_cache_mutex_);
        if (row_page_cache_.empty()) {
            return;
        }
    }

    const std::string prefix = database_name + "." + table_name + ":";
    std::lock_guard<std::mutex> pg(page_cache_mutex_);
    bool found = false;
    for (auto it = row_page_cache_.begin(); it != row_page_cache_.end(); ) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = row_page_cache_.erase(it);
            found = true;
        } else {
            ++it;
        }
    }
    if (found) {
        log_cache_event("INVALIDATE [page]", dep_key);
    }
}

}  // namespace flexql
