#include <flexql.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>

#include "client/flexql_internal.h"
#include "common/errors.h"
#include "utils/helpers.h"

namespace {

void assign_error(char **out_error, const std::string &text) {
    if (out_error == nullptr) {
        return;
    }

    char *owned_copy = static_cast<char *>(std::malloc(text.size() + 1));
    if (owned_copy == nullptr) {
        *out_error = nullptr;
        return;
    }

    std::memcpy(owned_copy, text.c_str(), text.size() + 1);
    *out_error = owned_copy;
}

std::vector<std::string> split_lines(const std::string &text) {
    std::vector<std::string> collected_lines;
    std::stringstream input_stream(text);
    std::string current_line;
    while (std::getline(input_stream, current_line)) {
        collected_lines.push_back(current_line);
    }
    return collected_lines;
}

bool receive_and_dispatch_response(
    FlexQL *handle,
    int (*callback)(void *, int, char **, char **),
    void *callback_arg,
    char **out_error) {
    std::string failure_reason;
    std::string response;
    if (!handle->socket_client.receive_message(response, failure_reason)) {
        assign_error(out_error, failure_reason);
        return false;
    }

    const auto response_lines = split_lines(response);
    if (response_lines.empty()) {
        assign_error(out_error, "empty server response");
        return false;
    }

    if (response_lines.front().rfind("ERR\t", 0) == 0) {
        assign_error(out_error, response_lines.front().substr(4));
        return false;
    }

    if (response_lines.front().rfind("OK\t", 0) == 0) {
        return true;
    }

    if (response_lines.front().rfind("RESULT\t", 0) != 0) {
        assign_error(out_error, "malformed server response");
        return false;
    }

    if (callback == nullptr) {
        return true;
    }

    if (response_lines.size() < 2) {
        assign_error(out_error, "missing header row");
        return false;
    }

    const auto header_values = flexql::split(response_lines[1], '\t');
    std::vector<std::unique_ptr<char[]>> header_storage;
    std::vector<char *> header_ptrs;
    for (const auto &column_name : header_values) {
        auto owned_text = std::make_unique<char[]>(column_name.size() + 1);
        std::memcpy(owned_text.get(), column_name.c_str(), column_name.size() + 1);
        header_ptrs.push_back(owned_text.get());
        header_storage.push_back(std::move(owned_text));
    }

    for (std::size_t row_index = 2; row_index < response_lines.size(); ++row_index) {
        const auto row_values = flexql::split(response_lines[row_index], '\t');
        std::vector<std::unique_ptr<char[]>> value_storage;
        std::vector<char *> value_ptrs;
        for (const auto &field : row_values) {
            auto owned_text = std::make_unique<char[]>(field.size() + 1);
            std::memcpy(owned_text.get(), field.c_str(), field.size() + 1);
            value_ptrs.push_back(owned_text.get());
            value_storage.push_back(std::move(owned_text));
        }
        if (callback(
                callback_arg,
                static_cast<int>(value_ptrs.size()),
                value_ptrs.data(),
                header_ptrs.data()) != 0) {
            break;
        }
    }

    return true;
}

bool send_payload(FlexQL *handle, const std::string &payload, char **out_error) {
    std::string failure_reason;
    if (!handle->socket_client.send_message(payload, failure_reason)) {
        assign_error(out_error, failure_reason);
        return false;
    }
    return true;
}

}  // namespace

int flexql_open(const char *host_name, int port_number, FlexQL **out_db) {
    if (host_name == nullptr || out_db == nullptr) {
        return flexql::FLEXQL_ERROR;
    }

    std::unique_ptr<FlexQL> handle(new FlexQL());
    std::string failure_reason;
    if (!handle->socket_client.connect_to(host_name, port_number, failure_reason)) {
        return flexql::FLEXQL_NETWORK_ERROR;
    }

    handle->is_open = true;
    *out_db = handle.release();
    return flexql::FLEXQL_OK;
}

int flexql_close(FlexQL *handle) {
    if (handle == nullptr) {
        return flexql::FLEXQL_ERROR;
    }

    {
        std::lock_guard<std::mutex> guard(handle->state_mutex);
        handle->socket_client.close();
        handle->is_open = false;
    }

    delete handle;
    return flexql::FLEXQL_OK;
}

int flexql_exec(
    FlexQL *handle,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *callback_arg,
    char **out_error) {
    if (handle == nullptr || sql == nullptr || !handle->is_open) {
        assign_error(out_error, "invalid database handle");
        return flexql::FLEXQL_ERROR;
    }

    std::lock_guard<std::mutex> guard(handle->state_mutex);
    if (!send_payload(handle, sql, out_error)) {
        return flexql::FLEXQL_NETWORK_ERROR;
    }
    return receive_and_dispatch_response(handle, callback, callback_arg, out_error)
        ? flexql::FLEXQL_OK
        : flexql::FLEXQL_ERROR;
}

int flexql_exec_batch(
    FlexQL *handle,
    const char *const *sql_statements,
    int statement_count,
    char **out_error) {
    if (handle == nullptr || !handle->is_open) {
        assign_error(out_error, "invalid database handle");
        return flexql::FLEXQL_ERROR;
    }
    if (sql_statements == nullptr || statement_count <= 0) {
        assign_error(out_error, "invalid batch arguments");
        return flexql::FLEXQL_ERROR;
    }

    std::string payload = "MSG_BATCH_INSERT\t" + std::to_string(statement_count) + "\n";
    for (int index = 0; index < statement_count; ++index) {
        if (sql_statements[index] == nullptr) {
            assign_error(out_error, "null SQL statement in batch");
            return flexql::FLEXQL_ERROR;
        }
        std::string statement(sql_statements[index]);
        if (statement.find('\n') != std::string::npos) {
            assign_error(out_error, "batch statements must not contain newlines");
            return flexql::FLEXQL_ERROR;
        }
        payload += statement;
        payload += '\n';
    }

    std::lock_guard<std::mutex> guard(handle->state_mutex);
    if (!send_payload(handle, payload, out_error)) {
        return flexql::FLEXQL_NETWORK_ERROR;
    }
    return receive_and_dispatch_response(handle, nullptr, nullptr, out_error)
        ? flexql::FLEXQL_OK
        : flexql::FLEXQL_ERROR;
}

void flexql_free(void *ptr) {
    std::free(ptr);
}
