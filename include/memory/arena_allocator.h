#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <vector>

namespace flexql {

class ArenaAllocator : public std::pmr::memory_resource {
public:
    explicit ArenaAllocator(std::size_t default_block_size = 64 * 1024);
    ~ArenaAllocator() override = default;

    void reset();

protected:
    void *do_allocate(std::size_t bytes, std::size_t alignment) override;
    void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) override;
    bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override;

private:
    struct Block {
        std::unique_ptr<std::byte[]> data;
        std::size_t capacity = 0;
        std::size_t used = 0;
    };

    void add_block(std::size_t minimum_capacity);

    std::size_t block_size_;
    std::vector<Block> blocks_;
};

}  // namespace flexql
