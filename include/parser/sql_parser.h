#pragma once

#include <string>

#include "common/types.h"

namespace flexql {

class SqlParser {
public:
    bool parse(const std::string &sql_text, Query &parsed_query, std::string &error_message) const;

private:
    bool parse_create_database(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
    bool parse_use_database(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
    bool parse_create(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
    bool parse_drop_table(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
    bool parse_insert(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
    bool parse_select(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
    bool parse_show(
        const std::string &sql_text,
        Query &parsed_query,
        std::string &error_message) const;
};

}  // namespace flexql
