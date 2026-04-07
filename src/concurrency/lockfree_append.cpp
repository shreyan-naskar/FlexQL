#include "concurrency/lockfree_append.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace flexql {

namespace {
constexpr std::uint64_t kPreallocChunk = 64ULL * 1024 * 1024;
}

LockFreeAppendFile::~LockFreeAppendFile() {
    close();
}

bool LockFreeAppendFile::open(const std::string &file_path, std::string &error_message) {
    if (fd_ >= 0) {
        return true;
    }

    const int opened_fd = ::open(file_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (opened_fd < 0) {
        error_message = "failed to open append file";
        return false;
    }

    struct stat file_stats {};
    if (::fstat(opened_fd, &file_stats) != 0) {
        error_message = "failed to stat append file";
        ::close(opened_fd);
        return false;
    }

    fd_ = opened_fd;
    const std::uint64_t existing_size = static_cast<std::uint64_t>(file_stats.st_size);
    append_offset_.store(existing_size, std::memory_order_release);
    allocated_size_.store(existing_size, std::memory_order_release);
    return true;
}

void LockFreeAppendFile::close() {
    if (fd_ >= 0) {
        const std::uint64_t committed_size = append_offset_.load(std::memory_order_acquire);
        const int truncate_status = ::ftruncate(fd_, static_cast<off_t>(committed_size));
        (void)truncate_status;
        ::close(fd_);
        fd_ = -1;
    }

    append_offset_.store(0, std::memory_order_release);
    allocated_size_.store(0, std::memory_order_release);
}

bool LockFreeAppendFile::is_open() const {
    return fd_ >= 0;
}

void LockFreeAppendFile::maybe_extend(std::uint64_t required_end_offset) {
    if (required_end_offset <= allocated_size_.load(std::memory_order_acquire)) {
        return;
    }

    std::lock_guard<std::mutex> guard(fallocate_mutex_);
    const std::uint64_t allocated_bytes = allocated_size_.load(std::memory_order_acquire);
    if (required_end_offset <= allocated_bytes) {
        return;
    }

    const std::uint64_t expanded_size =
        ((required_end_offset + kPreallocChunk - 1) / kPreallocChunk) * kPreallocChunk;
    if (::fallocate(
            fd_,
            FALLOC_FL_KEEP_SIZE,
            static_cast<off_t>(allocated_bytes),
            static_cast<off_t>(expanded_size - allocated_bytes)) == 0) {
        allocated_size_.store(expanded_size, std::memory_order_release);
    } else {
        allocated_size_.store(required_end_offset, std::memory_order_release);
    }
}

std::uint64_t LockFreeAppendFile::reserve(std::size_t byte_count) {
    const std::uint64_t reserved_offset = append_offset_.fetch_add(
        static_cast<std::uint64_t>(byte_count),
        std::memory_order_acq_rel);
    maybe_extend(reserved_offset + static_cast<std::uint64_t>(byte_count));
    return reserved_offset;
}

bool LockFreeAppendFile::write_at(
    std::uint64_t file_offset,
    std::string_view payload,
    std::string &error_message) {
    if (fd_ < 0) {
        error_message = "append file is not open";
        return false;
    }

    std::size_t bytes_written = 0;
    while (bytes_written < payload.size()) {
        const ssize_t write_result = ::pwrite(
            fd_,
            payload.data() + bytes_written,
            payload.size() - bytes_written,
            static_cast<off_t>(file_offset + bytes_written));
        if (write_result < 0) {
            if (errno == EINTR) {
                continue;
            }
            error_message = "failed to append row";
            return false;
        }

        bytes_written += static_cast<std::size_t>(write_result);
    }
    return true;
}

std::uint64_t LockFreeAppendFile::size() const {
    return append_offset_.load(std::memory_order_acquire);
}

}  // namespace flexql
