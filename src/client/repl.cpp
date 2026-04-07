#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "client/flexql_internal.h"
#include <flexql.h>
#include "utils/helpers.h"

namespace flexql {

namespace {
std::vector<std::string> split_lines(const std::string &text) {
    std::vector<std::string> collected_lines;
    std::stringstream input_stream(text);
    std::string current_line;
    while (std::getline(input_stream, current_line)) {
        collected_lines.push_back(current_line);
    }
    return collected_lines;
}

void print_separator(const std::vector<std::size_t> &column_widths) {
    std::cout << '+';
    for (std::size_t width : column_widths) {
        std::cout << std::string(width + 2, '-') << '+';
    }
    std::cout << '\n';
}

void print_table(
    const std::vector<std::string> &headers,
    const std::vector<std::vector<std::string>> &rows) {
    std::vector<std::size_t> column_widths(headers.size(), 0);
    for (std::size_t column_index = 0; column_index < headers.size(); ++column_index) {
        column_widths[column_index] = headers[column_index].size();
    }

    for (const auto &record : rows) {
        for (std::size_t column_index = 0;
             column_index < record.size() && column_index < column_widths.size();
             ++column_index) {
            column_widths[column_index] = std::max(
                column_widths[column_index],
                record[column_index].size());
        }
    }

    print_separator(column_widths);
    std::cout << '|';
    for (std::size_t column_index = 0; column_index < headers.size(); ++column_index) {
        std::cout << ' '
                  << std::left
                  << std::setw(static_cast<int>(column_widths[column_index]))
                  << headers[column_index]
                  << " |";
    }
    std::cout << '\n';
    print_separator(column_widths);

    for (const auto &record : rows) {
        std::cout << '|';
        for (std::size_t column_index = 0; column_index < headers.size(); ++column_index) {
            const std::string display_value =
                column_index < record.size() ? record[column_index] : "";
            std::cout << ' '
                      << std::left
                      << std::setw(static_cast<int>(column_widths[column_index]))
                      << display_value
                      << " |";
        }
        std::cout << '\n';
    }

    print_separator(column_widths);
    std::cout << rows.size() << (rows.size() == 1 ? " row" : " rows") << " in set.\n";
}

bool execute_and_print(FlexQL *session, const std::string &sql_text) {
    std::lock_guard<std::mutex> guard(session->state_mutex);
    std::string failure_reason;
    if (!session->socket_client.send_message(sql_text, failure_reason)) {
        std::cerr << "error: " << failure_reason << '\n';
        return false;
    }

    std::string response;
    if (!session->socket_client.receive_message(response, failure_reason)) {
        std::cerr << "error: " << failure_reason << '\n';
        return false;
    }

    const auto response_lines = split_lines(response);
    if (response_lines.empty()) {
        std::cerr << "error: empty server response\n";
        return false;
    }

    if (response_lines.front().rfind("ERR\t", 0) == 0) {
        std::cerr << "error: " << response_lines.front().substr(4) << '\n';
        return false;
    }

    if (response_lines.front().rfind("OK\t", 0) == 0) {
        std::cout << response_lines.front().substr(3) << '\n';
        return true;
    }

    if (response_lines.front().rfind("RESULT\t", 0) != 0 || response_lines.size() < 2) {
        std::cerr << "error: malformed server response\n";
        return false;
    }

    const auto headers = split(response_lines[1], '\t');
    std::vector<std::vector<std::string>> rows;
    for (std::size_t row_index = 2; row_index < response_lines.size(); ++row_index) {
        rows.push_back(split(response_lines[row_index], '\t'));
    }
    print_table(headers, rows);
    return true;
}

void print_statement_timing(int statement_number, int line_number, long long elapsed_ms) {
    std::cout << "[SOURCE] statement " << statement_number
              << " (line " << line_number << ") finished in "
              << elapsed_ms << " ms.\n";
}

void print_query_timing(long long elapsed_ms) {
    std::cout << "[QUERY] finished in " << elapsed_ms << " ms.\n";
}

bool execute_source_file(FlexQL *session, const std::string &source_path) {
    std::ifstream source_stream(source_path);
    if (!source_stream) {
        std::cerr << "error: could not open source file '" << source_path << "'\n";
        return false;
    }

    std::string raw_line;
    std::string current_statement;
    int executed_count = 0;
    int current_line_number = 0;
    const auto source_begin = std::chrono::steady_clock::now();

    while (std::getline(source_stream, raw_line)) {
        ++current_line_number;
        const std::string cleaned_line = trim(raw_line);
        if (cleaned_line.empty() || cleaned_line.rfind("--", 0) == 0) {
            continue;
        }

        if (!current_statement.empty()) {
            current_statement.push_back(' ');
        }
        current_statement += cleaned_line;

        if (cleaned_line.find(';') == std::string::npos) {
            continue;
        }

        const auto started_at = std::chrono::steady_clock::now();
        if (!execute_and_print(session, current_statement)) {
            const auto finished_at = std::chrono::steady_clock::now();
            const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                finished_at - started_at).count();
            print_statement_timing(executed_count + 1, current_line_number, elapsed_ms);
            std::cerr << "source stopped at line " << current_line_number << '\n';
            return false;
        }

        ++executed_count;
        const auto finished_at = std::chrono::steady_clock::now();
        const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(finished_at - started_at).count();
        print_statement_timing(executed_count, current_line_number, elapsed_ms);
        current_statement.clear();
    }

    if (!trim(current_statement).empty()) {
        const auto started_at = std::chrono::steady_clock::now();
        if (!execute_and_print(session, current_statement)) {
            const auto finished_at = std::chrono::steady_clock::now();
            const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                finished_at - started_at).count();
            print_statement_timing(executed_count + 1, current_line_number, elapsed_ms);
            std::cerr << "source stopped at end of file\n";
            return false;
        }

        ++executed_count;
        const auto finished_at = std::chrono::steady_clock::now();
        const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(finished_at - started_at).count();
        print_statement_timing(executed_count, current_line_number, elapsed_ms);
    }

    const auto source_end = std::chrono::steady_clock::now();
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(source_end - source_begin).count();
    std::cout << "SOURCE completed. " << executed_count
              << " statements executed in " << elapsed_ms << " ms.\n";
    return true;
}

}  // namespace

int run_repl(FlexQL *session) {
    std::string input_line;
    for (;;) {
        std::cout << "flexql> " << std::flush;
        if (!std::getline(std::cin, input_line)) {
            break;
        }

        if (input_line == ".exit") {
            break;
        }
        if (input_line.empty()) {
            continue;
        }

        const std::string cleaned_line = trim(input_line);
        const std::string upper_line = to_upper(cleaned_line);
        if (upper_line.rfind("SOURCE ", 0) == 0) {
            std::string source_path = trim(cleaned_line.substr(7));
            if (!source_path.empty() && source_path.back() == ';') {
                source_path.pop_back();
                source_path = trim(source_path);
            }
            execute_source_file(session, source_path);
            continue;
        }

        const auto started_at = std::chrono::steady_clock::now();
        execute_and_print(session, input_line);
        const auto finished_at = std::chrono::steady_clock::now();
        const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(finished_at - started_at).count();
        print_query_timing(elapsed_ms);
    }
    return 0;
}

}  // namespace flexql
