#include "memory/arena_allocator.h"

#include <algorithm>
#include <cstdint>

namespace flexql {

ArenaAllocator::ArenaAllocator(std::size_t default_block_size)
    : block_size_(default_block_size) {}

void ArenaAllocator::reset() {
    for (auto &chunk : blocks_) {
        chunk.used = 0;
    }
}

void *ArenaAllocator::do_allocate(std::size_t requested_bytes, std::size_t requested_alignment) {
    if (requested_bytes == 0) {
        requested_bytes = 1;
    }

    for (auto &chunk : blocks_) {
        const std::uintptr_t chunk_start =
            reinterpret_cast<std::uintptr_t>(chunk.data.get()) + chunk.used;
        const std::size_t unused_capacity = chunk.capacity - chunk.used;
        const std::size_t aligned_address =
            (chunk_start + requested_alignment - 1) & ~(requested_alignment - 1);
        const std::size_t padding_bytes = aligned_address - chunk_start;
        if (padding_bytes + requested_bytes <= unused_capacity) {
            chunk.used += padding_bytes + requested_bytes;
            return reinterpret_cast<void *>(aligned_address);
        }
    }

    add_block(requested_bytes + requested_alignment);
    return do_allocate(requested_bytes, requested_alignment);
}

void ArenaAllocator::do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) {
    (void)pointer;
    (void)bytes;
    (void)alignment;
}

bool ArenaAllocator::do_is_equal(const std::pmr::memory_resource &other) const noexcept {
    return this == &other;
}

void ArenaAllocator::add_block(std::size_t minimum_capacity) {
    const std::size_t actual_capacity = std::max(block_size_, minimum_capacity);
    Block fresh_block;
    fresh_block.data = std::make_unique<std::byte[]>(actual_capacity);
    fresh_block.capacity = actual_capacity;
    fresh_block.used = 0;
    blocks_.push_back(std::move(fresh_block));
}

}  // namespace flexql
