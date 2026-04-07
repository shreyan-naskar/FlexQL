#pragma once

#include <memory>
#include <mutex>
#include <string_view>
#include <string>
#include <unordered_map>

#include "cache/lru_cache.h"
#include "expiration/ttl_manager.h"
#include "index/btree_index.h"
#include "parser/sql_parser.h"
#include "query/query_plan.h"
#include "storage/table.h"

namespace flexql {

class Executor {
public:
    Executor(const std::string &root_directory, std::shared_ptr<LruCache> query_cache);

    QueryResult execute_sql(const std::string &sql_text, std::string &current_database);
    QueryResult execute_batch_insert(
        const std::vector<std::string> &statements,
        std::string &current_database);

private:
    QueryResult execute(const QueryPlan &compiled_plan);
    std::shared_ptr<Table> get_table(
        const std::string &database_name,
        const std::string &table_name,
        std::string &error_message);
    QueryResult execute_create_database(const Query &parsed_query);
    QueryResult execute_use_database(const Query &parsed_query);
    QueryResult execute_create(const Query &parsed_query);
    QueryResult execute_drop_table(const Query &parsed_query);
    QueryResult execute_insert(const Query &parsed_query);
    QueryResult execute_select(const Query &parsed_query);
    QueryResult execute_show_databases();
    QueryResult execute_show_tables(const Query &parsed_query);
    QueryResult execute_simple_select(
        Table &table,
        const Query &parsed_query,
        const std::string &normalized_sql);
    QueryResult execute_join_select(
        Table &left_table,
        const Query &parsed_query,
        const std::string &normalized_sql);
    void invalidate_table_cache(const std::string &database_name, const std::string &table_name);

    // Page cache helpers (spatial locality).
    // Returns true when pages are already loaded (HIT) or successfully loaded
    // from disk (MISS).  Logs "CACHE HIT [page]" / "CACHE MISS [page]".
    bool ensure_pages_loaded(
        const std::string &database_name,
        const std::string &table_name,
        Table &table,
        std::string &error_message);

    // Collect all rows for a table from the page cache into a flat vector.
    void collect_page_rows(
        const std::string &database_name,
        const std::string &table_name,
        std::vector<Row> &rows_out) const;

    SqlParser parser_;
    std::string data_root_;
    std::shared_ptr<LruCache> cache_;
    TtlManager ttl_manager_;
    std::mutex table_cache_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Table>> table_cache_;

    // Row-level page cache: key = "db.table:page_id", value = rows in that page.
    // Provides spatial locality — adjacent rows are loaded together and reused
    // across queries without hitting disk again.
    static constexpr int kPageSize = 64;
    mutable std::mutex page_cache_mutex_;
    std::unordered_map<std::string, std::vector<Row>> row_page_cache_;
};

}  // namespace flexql
