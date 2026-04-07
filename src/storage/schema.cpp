#include "storage/schema.h"

#include <filesystem>
#include <fstream>

#include "utils/helpers.h"

namespace flexql {

Schema::Schema(std::string database_label, std::string table_label)
    : database_name_(std::move(database_label)),
      table_name_(std::move(table_label)) {}

const std::string &Schema::database_name() const {
    return database_name_;
}

const std::string &Schema::table_name() const {
    return table_name_;
}

const std::vector<ColumnDef> &Schema::columns() const {
    return columns_;
}

void Schema::set_columns(std::vector<ColumnDef> declared_columns) {
    columns_ = std::move(declared_columns);
}

int Schema::primary_key_index() const {
    for (std::size_t column_index = 0; column_index < columns_.size(); ++column_index) {
        if (columns_[column_index].primary_key) {
            return static_cast<int>(column_index);
        }
    }

    return columns_.empty() ? -1 : 0;
}

std::string Schema::primary_key_name() const {
    const int primary_index = primary_key_index();
    if (primary_index < 0) {
        return "";
    }

    return columns_[primary_index].name;
}

int Schema::column_index(const std::string &column_name) const {
    const std::string normalized_name = to_upper(column_name);
    for (std::size_t column_index = 0; column_index < columns_.size(); ++column_index) {
        if (to_upper(columns_[column_index].name) == normalized_name) {
            return static_cast<int>(column_index);
        }
    }

    return -1;
}

bool Schema::save(const std::string &root_path, std::string &failure_reason) const {
    const std::string schema_dir =
        root_path + "/data/databases/" + database_name_ + "/tables";
    std::filesystem::create_directories(schema_dir);

    std::ofstream output_stream(schema_dir + "/" + table_name_ + ".schema", std::ios::trunc);
    if (!output_stream) {
        failure_reason = "failed to write schema";
        return false;
    }

    output_stream << database_name_ << '\n';
    output_stream << table_name_ << '\n';
    output_stream << columns_.size() << '\n';
    for (const auto &column_def : columns_) {
        output_stream << column_def.name
                      << '|'
                      << data_type_to_string(column_def.type)
                      << '|'
                      << (column_def.primary_key ? "1" : "0")
                      << '\n';
    }
    return true;
}

bool Schema::load(
    const std::string &root_path,
    const std::string &database_label,
    const std::string &table_label,
    std::string &failure_reason) {
    database_name_ = database_label;
    table_name_ = table_label;

    std::ifstream input_stream(
        root_path + "/data/databases/" + database_label + "/tables/" + table_label + ".schema");
    if (!input_stream) {
        failure_reason = "schema not found";
        return false;
    }

    std::string raw_line;
    std::getline(input_stream, database_name_);
    std::getline(input_stream, table_name_);
    std::getline(input_stream, raw_line);

    columns_.clear();
    const int row_count = std::stoi(raw_line);
    for (int row_index = 0; row_index < row_count; ++row_index) {
        std::getline(input_stream, raw_line);
        const auto fields = split(raw_line, '|');
        if (fields.size() != 2 && fields.size() != 3) {
            failure_reason = "invalid schema row";
            return false;
        }

        DataType parsed_type;
        if (!data_type_from_string(fields[1], parsed_type)) {
            failure_reason = "invalid schema type";
            return false;
        }

        const bool is_primary_key = fields.size() == 3 && fields[2] == "1";
        columns_.push_back({fields[0], parsed_type, is_primary_key});
    }
    return true;
}

}  // namespace flexql
