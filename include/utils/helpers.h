#pragma once

#include <ctime>
#include <string>
#include <vector>

#include "common/types.h"

namespace flexql {

std::string trim(const std::string &raw_text);
std::string to_upper(std::string raw_text);
std::string normalize_identifier(const std::string &identifier);
std::vector<std::string> split_csv(const std::string &csv_text);
std::vector<std::string> split(const std::string &text, char delimiter);
std::string join(const std::vector<std::string> &items, const std::string &separator);
std::string normalize_sql(const std::string &sql_text);
bool validate_value_for_type(const std::string &raw_value, DataType type);
std::string data_type_to_string(DataType type);
bool data_type_from_string(const std::string &type_name, DataType &type);
std::string escape_field(const std::string &field_value);
std::string unescape_field(const std::string &field_value);
bool parse_datetime_value(const std::string &raw_value, std::time_t &timestamp);
bool compare_values(
    const std::string &lhs,
    const std::string &rhs,
    DataType type,
    ComparisonOp op);

}  // namespace flexql
