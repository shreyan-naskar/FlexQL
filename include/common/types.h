#pragma once

#include <ctime>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace flexql {

enum class DataType {
    Int,
    Decimal,
    Varchar,
    Datetime
};

enum class QueryType {
    CreateDatabase,
    UseDatabase,
    CreateTable,
    DropTable,
    Insert,
    Select,
    ShowDatabases,
    ShowTables,
    Unknown
};

enum class ComparisonOp {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual
};

struct ColumnDef {
    std::string name;
    DataType type;
    bool primary_key = false;
};

struct Condition {
    bool enabled = false;
    std::string column;
    ComparisonOp op = ComparisonOp::Equal;
    std::string value;
};

struct JoinSpec {
    std::string right_table;
    std::string left_column;
    std::string right_column;
};

struct Query {
    QueryType type = QueryType::Unknown;
    std::string database;
    std::string table;
    bool if_exists = false;
    bool if_not_exists = false;
    std::vector<ColumnDef> column_defs;
    std::vector<std::string> insert_values;
    std::vector<std::vector<std::string>> insert_multi_values;
    std::time_t insert_expiration = 0;
    std::vector<std::string> select_columns;
    Condition where;
    std::vector<JoinSpec> joins;
};

struct Row {
    std::vector<std::string> values;
    std::time_t expiration = 0;
};

struct QueryResult {
    bool success = false;
    std::string message;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
    std::time_t cache_expires_at = 0;
};

using IndexMap = std::unordered_map<std::string, std::uint64_t>;

}  // namespace flexql
