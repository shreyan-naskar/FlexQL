#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FlexQL FlexQL;

int flexql_open(const char *host_name, int port_number, FlexQL **handle);

int flexql_close(FlexQL *handle);

int flexql_exec(
    FlexQL *handle,
    const char *sql_text,
    int (*cb)(void *, int, char **, char **),
    void *user_data,
    char **err_out);

int flexql_exec_batch(
    FlexQL *handle,
    const char *const *statements,
    int statement_count,
    char **err_out);

void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif
