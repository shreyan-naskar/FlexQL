#include "parser/sql_parser.h"

#include <regex>

#include "utils/helpers.h"

namespace flexql {
namespace {

bool parse_qualified_name(const std::string &input, std::string &database_name, std::string &table_name) {
    const auto name_parts = split(trim(input), '.');
    if (name_parts.size() == 1) {
        database_name.clear();
        table_name = normalize_identifier(name_parts[0]);
        return !table_name.empty();
    }
    if (name_parts.size() == 2) {
        database_name = normalize_identifier(name_parts[0]);
        table_name = normalize_identifier(name_parts[1]);
        return !database_name.empty() && !table_name.empty();
    }
    return false;
}

bool parse_condition(const std::string &input, Condition &condition) {
    static const std::regex pattern(
        R"(^\s*([A-Za-z_][A-Za-z0-9_\.]*)\s*(=|!=|<>|>=|<=|>|<)\s*(.+?)\s*$)",
        std::regex::icase);
    std::smatch captures;
    if (!std::regex_match(input, captures, pattern)) {
        return false;
    }
    condition.enabled = true;
    condition.column = captures[1].str();
    const std::string comparison = captures[2].str();
    if (comparison == "=") {
        condition.op = ComparisonOp::Equal;
    } else if (comparison == "!=" || comparison == "<>") {
        condition.op = ComparisonOp::NotEqual;
    } else if (comparison == ">") {
        condition.op = ComparisonOp::Greater;
    } else if (comparison == ">=") {
        condition.op = ComparisonOp::GreaterEqual;
    } else if (comparison == "<") {
        condition.op = ComparisonOp::Less;
    } else if (comparison == "<=") {
        condition.op = ComparisonOp::LessEqual;
    } else {
        return false;
    }
    condition.value = trim(captures[3].str());
    return true;
}

bool parse_insert_expiration(const std::string &value, std::time_t &expiration) {
    std::string normalized = trim(value);
    if (normalized.size() >= 2 && normalized.front() == '\'' && normalized.back() == '\'') {
        normalized = normalized.substr(1, normalized.size() - 2);
    }
    return parse_datetime_value(normalized, expiration);
}

std::string normalize_declared_type(const std::string &value) {
    static const std::regex re(R"(^([A-Za-z]+)(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?$)", std::regex::icase);
    std::smatch match;
    const std::string trimmed = trim(value);
    if (std::regex_match(trimmed, match, re)) {
        return match[1].str();
    }
    return trimmed;
}

}  // namespace

bool SqlParser::parse(const std::string &sql, Query &query, std::string &error) const {
    const std::string normalized_sql = normalize_sql(sql);
    // Only uppercase the prefix needed for statement-type dispatch — avoids an O(N)
    // full-string copy on large multi-row INSERT payloads (which can be 100 KB+).
    // The longest keyword we match is "CREATE DATABASE" = 15 chars; 16 is sufficient.
    constexpr std::size_t kDispatchPrefixLen = 16;
    const std::string upper_prefix = to_upper(
        normalized_sql.substr(0, std::min(normalized_sql.size(), kDispatchPrefixLen)));
    if (upper_prefix.rfind("CREATE DATABASE", 0) == 0) {
        return parse_create_database(normalized_sql, query, error);
    }
    if (upper_prefix.rfind("USE ", 0) == 0) {
        return parse_use_database(normalized_sql, query, error);
    }
    if (upper_prefix.rfind("CREATE TABLE", 0) == 0) {
        return parse_create(normalized_sql, query, error);
    }
    if (upper_prefix.rfind("DROP TABLE", 0) == 0) {
        return parse_drop_table(normalized_sql, query, error);
    }
    if (upper_prefix.rfind("INSERT INTO", 0) == 0) {
        return parse_insert(normalized_sql, query, error);
    }
    if (upper_prefix.rfind("SHOW ", 0) == 0) {
        return parse_show(normalized_sql, query, error);
    }
    if (upper_prefix.rfind("SELECT", 0) == 0) {
        return parse_select(normalized_sql, query, error);
    }
    error = "unsupported SQL statement";
    return false;
}

bool SqlParser::parse_create_database(const std::string &sql, Query &query, std::string &error) const {
    static const std::regex re(
        R"(^CREATE\s+DATABASE(?:\s+IF\s+NOT\s+EXISTS)?\s+([A-Za-z_][A-Za-z0-9_]*)$)",
        std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, re)) {
        error = "invalid CREATE DATABASE syntax";
        return false;
    }

    query = {};
    query.type = QueryType::CreateDatabase;
    query.database = normalize_identifier(match[1].str());
    query.if_not_exists = to_upper(sql).find("CREATE DATABASE IF NOT EXISTS") == 0;
    return true;
}

bool SqlParser::parse_use_database(const std::string &sql, Query &query, std::string &error) const {
    static const std::regex re(R"(^USE\s+([A-Za-z_][A-Za-z0-9_]*)$)", std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, re)) {
        error = "invalid USE syntax";
        return false;
    }

    query = {};
    query.type = QueryType::UseDatabase;
    query.database = normalize_identifier(match[1].str());
    return true;
}

bool SqlParser::parse_create(const std::string &sql, Query &query, std::string &error) const {
    static const std::regex re(
        R"(^CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*\((.+)\)$)",
        std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, re)) {
        error = "invalid CREATE TABLE syntax";
        return false;
    }

    query = {};
    query.type = QueryType::CreateTable;
    query.if_not_exists = to_upper(sql).find("CREATE TABLE IF NOT EXISTS") == 0;
    if (!parse_qualified_name(match[1].str(), query.database, query.table)) {
        error = "invalid table name";
        return false;
    }

    std::string declared_primary_key;
    for (const auto &part : split_csv(match[2].str())) {
        const std::string trimmed_part = trim(part);
        std::smatch pk_match;
        static const std::regex table_pk_re(R"(^PRIMARY\s+KEY\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)$)", std::regex::icase);
        if (std::regex_match(trimmed_part, pk_match, table_pk_re)) {
            declared_primary_key = pk_match[1].str();
            continue;
        }

        auto pieces = split(trimmed_part, ' ');
        std::vector<std::string> compact_pieces;
        for (const auto &piece : pieces) {
            if (!piece.empty()) {
                compact_pieces.push_back(piece);
            }
        }
        if (compact_pieces.size() < 2 || compact_pieces.size() > 4) {
            error = "invalid column definition";
            return false;
        }
        DataType type;
        if (!data_type_from_string(normalize_declared_type(compact_pieces[1]), type)) {
            error = "unsupported data type";
            return false;
        }

        bool primary_key = false;
        if (compact_pieces.size() > 2) {
            const std::string suffix = to_upper(join(std::vector<std::string>(compact_pieces.begin() + 2, compact_pieces.end()), " "));
            if (suffix != "PRIMARY KEY") {
                error = "unsupported column constraint";
                return false;
            }
            primary_key = true;
        }

        query.column_defs.push_back({compact_pieces[0], type, primary_key});
    }

    if (!declared_primary_key.empty()) {
        bool found = false;
        for (auto &column : query.column_defs) {
            if (to_upper(column.name) == to_upper(declared_primary_key)) {
                column.primary_key = true;
                found = true;
            }
        }
        if (!found) {
            error = "PRIMARY KEY column not found";
            return false;
        }
    }

    int primary_key_count = 0;
    for (const auto &column : query.column_defs) {
        if (column.primary_key) {
            ++primary_key_count;
        }
    }
    if (primary_key_count > 1) {
        error = "only one PRIMARY KEY is supported";
        return false;
    }
    return !query.column_defs.empty();
}

bool SqlParser::parse_drop_table(const std::string &sql, Query &query, std::string &error) const {
    static const std::regex re(
        R"(^DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+([A-Za-z_][A-Za-z0-9_\.]*)$)",
        std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, re)) {
        error = "invalid DROP TABLE syntax";
        return false;
    }

    query = {};
    query.type = QueryType::DropTable;
    query.if_exists = to_upper(sql).find("DROP TABLE IF EXISTS") == 0;
    if (!parse_qualified_name(match[1].str(), query.database, query.table)) {
        error = "invalid table name";
        return false;
    }
    return true;
}

bool SqlParser::parse_insert(const std::string &sql, Query &query, std::string &error) const {
    // regex_search on the header only — avoids applying a full-string regex to large VALUES payloads,
    // which would overflow the C++ regex engine's recursion stack on long multi-row INSERTs.
    static const std::regex header_re(
        R"(^INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_\.]*)\s+VALUES\s*)",
        std::regex::icase);
    static const std::regex expires_re(R"(^EXPIRES\s+AT\s+(.+)$)", std::regex::icase);

    std::smatch header_match;
    if (!std::regex_search(sql, header_match, header_re) || header_match.prefix().length() != 0) {
        error = "invalid INSERT syntax";
        return false;
    }

    query = {};
    query.type = QueryType::Insert;
    if (!parse_qualified_name(header_match[1].str(), query.database, query.table)) {
        error = "invalid table name";
        return false;
    }

    // Walk the values section, extracting each (...) group while respecting quoted strings.
    // header_match.suffix() is everything after "INSERT INTO table VALUES " — may be 100KB+ for batch inserts.
    std::string rest = trim(header_match.suffix().str());
    std::vector<std::vector<std::string>> all_rows;
    std::size_t pos = 0;

    while (pos < rest.size()) {
        // Skip whitespace and commas between groups.
        while (pos < rest.size() && (rest[pos] == ' ' || rest[pos] == ',')) {
            ++pos;
        }
        if (pos >= rest.size()) {
            break;
        }

        // EXPIRES AT is only valid after a single row group.
        // Use a simple case-insensitive prefix check instead of to_upper(substr) on the full tail.
        const bool starts_with_expires =
            pos + 7 <= rest.size() &&
            (rest[pos] == 'E' || rest[pos] == 'e') &&
            (rest[pos+1] == 'X' || rest[pos+1] == 'x') &&
            (rest[pos+2] == 'P' || rest[pos+2] == 'p') &&
            (rest[pos+3] == 'I' || rest[pos+3] == 'i') &&
            (rest[pos+4] == 'R' || rest[pos+4] == 'r') &&
            (rest[pos+5] == 'E' || rest[pos+5] == 'e') &&
            (rest[pos+6] == 'S' || rest[pos+6] == 's');
        if (starts_with_expires) {
            if (all_rows.size() != 1) {
                error = "EXPIRES AT is only supported for single-row INSERT";
                return false;
            }
            std::smatch exp_match;
            const std::string tail = rest.substr(pos);  // short tail: "EXPIRES AT '...'"
            if (!std::regex_match(tail, exp_match, expires_re) ||
                !parse_insert_expiration(exp_match[1].str(), query.insert_expiration)) {
                error = "invalid expiration timestamp";
                return false;
            }
            break;
        }

        if (rest[pos] != '(') {
            error = "expected '(' in VALUES list";
            return false;
        }
        ++pos;  // skip opening '('

        std::string group;
        bool in_quote = false;
        int depth = 1;
        while (pos < rest.size() && depth > 0) {
            const char c = rest[pos];
            if (c == '\'' && !in_quote) {
                in_quote = true;
                group += c;
            } else if (c == '\'' && in_quote) {
                in_quote = false;
                group += c;
            } else if (!in_quote && c == '(') {
                ++depth;
                group += c;
            } else if (!in_quote && c == ')') {
                --depth;
                if (depth > 0) {
                    group += c;
                }
            } else {
                group += c;
            }
            ++pos;
        }

        if (depth != 0) {
            error = "unmatched parentheses in VALUES";
            return false;
        }
        all_rows.push_back(split_csv(group));
    }

    if (all_rows.empty()) {
        error = "no values provided in INSERT";
        return false;
    }

    if (all_rows.size() == 1) {
        query.insert_values = all_rows[0];
    } else {
        query.insert_multi_values = std::move(all_rows);
    }
    return true;
}

bool SqlParser::parse_show(const std::string &sql, Query &query, std::string &error) const {
    static const std::regex show_db_re(R"(^SHOW\s+DATABASES$)", std::regex::icase);
    static const std::regex show_tables_re(R"(^SHOW\s+TABLES(?:\s+(?:FROM|IN)\s+([A-Za-z_][A-Za-z0-9_]*))?$)", std::regex::icase);

    std::smatch match;
    query = {};
    if (std::regex_match(sql, match, show_db_re)) {
        query.type = QueryType::ShowDatabases;
        return true;
    }
    if (std::regex_match(sql, match, show_tables_re)) {
        query.type = QueryType::ShowTables;
        query.database = match[1].matched ? normalize_identifier(match[1].str()) : "";
        return true;
    }

    error = "invalid SHOW syntax";
    return false;
}

bool SqlParser::parse_select(const std::string &sql, Query &query, std::string &error) const {
    static const std::regex select_re(
        R"(^SELECT\s+(.+?)\s+FROM\s+(.+?)(?:\s+WHERE\s+(.+))?$)",
        std::regex::icase);
    static const std::regex first_table_re(
        R"(^([A-Za-z_][A-Za-z0-9_\.]*)(.*)$)",
        std::regex::icase);
    static const std::regex join_clause_re(
        R"(^INNER\s+JOIN\s+([A-Za-z_][A-Za-z0-9_\.]*)\s+ON\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*=\s*([A-Za-z_][A-Za-z0-9_\.]*)(.*)$)",
        std::regex::icase);

    std::smatch match;
    query = {};
    query.type = QueryType::Select;

    if (!std::regex_match(sql, match, select_re)) {
        error = "invalid SELECT syntax";
        return false;
    }

    for (const auto &part : split_csv(match[1].str())) {
        query.select_columns.push_back(trim(part));
    }

    std::string from_clause = trim(match[2].str());
    if (match[3].matched && !parse_condition(match[3].str(), query.where)) {
        error = "invalid WHERE clause";
        return false;
    }

    std::smatch from_match;
    if (!std::regex_match(from_clause, from_match, first_table_re) ||
        !parse_qualified_name(from_match[1].str(), query.database, query.table)) {
        error = "invalid table name";
        return false;
    }

    std::string remaining = trim(from_match[2].str());
    while (!remaining.empty()) {
        std::smatch join_match;
        if (!std::regex_match(remaining, join_match, join_clause_re)) {
            error = "invalid SELECT syntax";
            return false;
        }

        std::string right_database;
        JoinSpec join_spec;
        if (!parse_qualified_name(join_match[1].str(), right_database, join_spec.right_table)) {
            error = "invalid right table name";
            return false;
        }
        if (!right_database.empty() && to_upper(right_database) != to_upper(query.database)) {
            error = "cross-database joins are not supported";
            return false;
        }

        join_spec.left_column = trim(join_match[2].str());
        join_spec.right_column = trim(join_match[3].str());
        query.joins.push_back(std::move(join_spec));
        remaining = trim(join_match[4].str());
    }

    return true;
}

}  // namespace flexql
