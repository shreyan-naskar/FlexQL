#include "storage/mmap_reader.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace flexql {

MmapReader::MmapReader() : fd_(-1), data_(nullptr), size_(0) {}

MmapReader::~MmapReader() {
    close();
}

bool MmapReader::open(const std::string &file_path, std::string &failure_reason) {
    close();

    const int opened_fd = ::open(file_path.c_str(), O_RDONLY);
    if (opened_fd < 0) {
        failure_reason = std::strerror(errno);
        return false;
    }
    fd_ = opened_fd;

    struct stat file_stats {};
    if (::fstat(fd_, &file_stats) < 0) {
        failure_reason = std::strerror(errno);
        close();
        return false;
    }

    size_ = static_cast<std::size_t>(file_stats.st_size);
    if (size_ == 0) {
        return true;
    }

    void *mapping = ::mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd_, 0);
    if (mapping == MAP_FAILED) {
        failure_reason = std::strerror(errno);
        close();
        return false;
    }

    data_ = static_cast<char *>(mapping);
    ::madvise(data_, size_, MADV_SEQUENTIAL);
    return true;
}

void MmapReader::close() {
    if (data_ != nullptr) {
        ::munmap(data_, size_);
        data_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    size_ = 0;
}

const char *MmapReader::data() const {
    return data_;
}

std::size_t MmapReader::size() const {
    return size_;
}

bool MmapReader::empty() const {
    return size_ == 0;
}

std::string_view MmapReader::view() const {
    if (data_ != nullptr && size_ != 0) {
        return std::string_view(data_, size_);
    }

    return {};
}

}  // namespace flexql
