#pragma once

namespace flexql {

enum ErrorCode {
    FLEXQL_OK           = 0,
    FLEXQL_ERROR        = 1,
    FLEXQL_NETWORK_ERROR = 2,
    FLEXQL_PARSE_ERROR  = 3,
    FLEXQL_STORAGE_ERROR = 4,
    FLEXQL_NOT_FOUND    = 5
};

}  // namespace flexql
