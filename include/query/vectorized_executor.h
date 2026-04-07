#pragma once

#include <string_view>

namespace flexql {

bool vectorized_equals(std::string_view left_view, std::string_view right_view);

}  // namespace flexql
