#include "query/vectorized_executor.h"

#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif

namespace flexql {

bool vectorized_equals(std::string_view left_view, std::string_view right_view) {
    const std::size_t view_length = left_view.size();
    if (view_length != right_view.size()) {
        return false;
    }

    if (view_length == 0) {
        return true;
    }

#if defined(__x86_64__) || defined(_M_X64)
    constexpr std::size_t kSimdWidth = 16;
    if (view_length >= kSimdWidth) {
        const char *left_ptr = left_view.data();
        const char *right_ptr = right_view.data();
        std::size_t cursor = 0;
        while (cursor + kSimdWidth <= view_length) {
            const __m128i left_chunk = _mm_loadu_si128(reinterpret_cast<const __m128i *>(left_ptr + cursor));
            const __m128i right_chunk = _mm_loadu_si128(reinterpret_cast<const __m128i *>(right_ptr + cursor));
            const __m128i equality_mask = _mm_cmpeq_epi8(left_chunk, right_chunk);
            if (_mm_movemask_epi8(equality_mask) != 0xFFFF) {
                return false;
            }

            cursor += kSimdWidth;
        }

        return std::memcmp(left_ptr + cursor, right_ptr + cursor, view_length - cursor) == 0;
    }
#endif

    return std::memcmp(left_view.data(), right_view.data(), view_length) == 0;
}

}  // namespace flexql
