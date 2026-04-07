#include "index/btree_index.h"

#include <filesystem>
#include <fstream>

#include "utils/helpers.h"

namespace flexql {

namespace {

constexpr std::uint32_t kSnapshotMagic = 0x58444E49U;  // INDX
constexpr std::uint32_t kDeltaMagic = 0x474F4C44U;     // DLOG
constexpr std::size_t kDeltaCompactThreshold = 4096;

template <typename T>
bool write_pod(std::ofstream &output_stream, const T &pod_value) {
    output_stream.write(reinterpret_cast<const char *>(&pod_value), sizeof(T));
    return static_cast<bool>(output_stream);
}

template <typename T>
bool read_pod(std::ifstream &input_stream, T &pod_value) {
    input_stream.read(reinterpret_cast<char *>(&pod_value), sizeof(T));
    return static_cast<bool>(input_stream);
}

}  // namespace

BTreeIndex::BTreeIndex(std::string database_label, std::string table_label)
    : database_name_(std::move(database_label)),
      table_name_(std::move(table_label)),
      bloom_filter_(8192, 3) {}

void BTreeIndex::set_location(const std::string &database_label, const std::string &table_label) {
    database_name_ = database_label;
    table_name_ = table_label;
}

bool BTreeIndex::load(const std::string &root_path, std::string &failure_reason) {
    entries_.clear();
    pending_delta_.clear();
    dirty_ = false;
    pending_updates_ = 0;
    bloom_filter_.reset(8192, 3);

    const std::string snapshot_file = snapshot_path(root_path);
    const std::string delta_file = delta_path(root_path);
    const std::string bloom_file = bloom_path(root_path);
    const std::string legacy_file = index_dir(root_path) + "/" + table_name_ + ".idx";

    if (std::filesystem::exists(snapshot_file)) {
        if (!load_snapshot(snapshot_file, failure_reason)) {
            return false;
        }
    } else if (std::filesystem::exists(legacy_file)) {
        if (!load_legacy_index(legacy_file, failure_reason)) {
            return false;
        }
    }

    if (std::filesystem::exists(delta_file) && !load_delta(delta_file, failure_reason)) {
        return false;
    }

    std::string bloom_failure;
    if (!bloom_filter_.load(bloom_file, bloom_failure)) {
        rebuild_bloom_filter();
    }
    return true;
}

bool BTreeIndex::save(const std::string &root_path, std::string &failure_reason) {
    // Hot path: only append the delta log — O(pending_delta_.size()), NOT O(N).
    // Never writes the full snapshot or rebuilds the bloom filter here.
    if (pending_delta_.empty()) {
        mark_flushed();
        return true;
    }

    std::filesystem::create_directories(index_dir(root_path));
    if (!append_delta(delta_path(root_path), pending_delta_, failure_reason)) {
        return false;
    }
    pending_delta_.clear();
    mark_flushed();
    return true;
}

bool BTreeIndex::compact(const std::string &root_path, std::string &failure_reason) {
    // Shutdown / explicit compaction: write full snapshot + bloom filter.
    // This is O(N) and should NEVER be called in the insert hot path.
    std::filesystem::create_directories(index_dir(root_path));
    if (!save_snapshot(snapshot_path(root_path), failure_reason)) {
        return false;
    }
    // Truncate the delta log — the snapshot now subsumes it.
    {
        std::ofstream truncate_stream(delta_path(root_path), std::ios::binary | std::ios::trunc);
        if (!truncate_stream) {
            failure_reason = "failed to clear delta log";
            return false;
        }
    }
    rebuild_bloom_filter();
    std::string bloom_failure;
    bloom_filter_.save(bloom_path(root_path), bloom_failure);
    pending_delta_.clear();
    mark_flushed();
    return true;
}

void BTreeIndex::drain_delta(std::vector<std::pair<std::string, std::uint64_t>> &drained_entries) {
    // Caller must hold write_state_mutex_. Moves pending_delta_ out so the
    // actual file I/O can happen outside any lock.
    drained_entries = std::move(pending_delta_);
    pending_delta_.clear();
    dirty_ = false;
    pending_updates_ = 0;
}

bool BTreeIndex::write_delta_entries(
    const std::string &root_path,
    const std::vector<std::pair<std::string, std::uint64_t>> &entries,
    std::string &failure_reason) const {
    // Pure file I/O on caller-supplied data — no shared mutable state accessed.
    // Serialize concurrent callers with index_flush_mutex_ in Table.
    if (entries.empty()) {
        return true;
    }

    std::filesystem::create_directories(index_dir(root_path));
    return append_delta(delta_path(root_path), entries, failure_reason);
}

void BTreeIndex::put(const std::string &key, std::uint64_t offset) {
    entries_[key] = offset;
    pending_delta_.push_back({key, offset});
    bloom_filter_.add(key);
    dirty_ = true;
    ++pending_updates_;
}

bool BTreeIndex::get(const std::string &key, std::uint64_t &offset) const {
    if (!bloom_filter_.might_contain(key)) {
        return false;
    }
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }
    offset = it->second;
    return true;
}

const IndexMap &BTreeIndex::entries() const {
    return entries_;
}

bool BTreeIndex::needs_flush() const {
    return dirty_ && pending_updates_ >= 4096;
}

void BTreeIndex::mark_flushed() {
    dirty_ = false;
    pending_updates_ = 0;
}

std::string BTreeIndex::index_dir(const std::string &root) const {
    return root + "/data/databases/" + database_name_ + "/indexes";
}

std::string BTreeIndex::snapshot_path(const std::string &root) const {
    return index_dir(root) + "/" + table_name_ + ".idx";
}

std::string BTreeIndex::delta_path(const std::string &root) const {
    return index_dir(root) + "/" + table_name_ + ".idx.delta";
}

std::string BTreeIndex::bloom_path(const std::string &root) const {
    return index_dir(root) + "/" + table_name_ + ".idx.bloom";
}

bool BTreeIndex::load_snapshot(const std::string &path, std::string &error) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        error = "snapshot not found";
        return false;
    }

    std::uint32_t magic = 0;
    if (!read_pod(in, magic)) {
        error = "invalid snapshot";
        return false;
    }
    if (magic != kSnapshotMagic) {
        in.close();
        return load_legacy_index(path, error);
    }

    std::uint64_t count = 0;
    if (!read_pod(in, count)) {
        error = "invalid snapshot";
        return false;
    }
    for (std::uint64_t i = 0; i < count; ++i) {
        std::uint32_t key_size = 0;
        std::uint64_t offset = 0;
        if (!read_pod(in, key_size) || key_size == 0) {
            error = "invalid snapshot row";
            return false;
        }
        std::string key(key_size, '\0');
        in.read(key.data(), static_cast<std::streamsize>(key_size));
        if (!read_pod(in, offset)) {
            error = "invalid snapshot row";
            return false;
        }
        entries_[std::move(key)] = offset;
    }
    return true;
}

bool BTreeIndex::load_delta(const std::string &path, std::string &error) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return true;
    }

    std::uint32_t magic = 0;
    if (!read_pod(in, magic) || magic != kDeltaMagic) {
        error = "invalid delta log";
        return false;
    }
    while (in.peek() != EOF) {
        std::uint32_t key_size = 0;
        std::uint64_t offset = 0;
        if (!read_pod(in, key_size)) {
            break;
        }
        std::string key(key_size, '\0');
        in.read(key.data(), static_cast<std::streamsize>(key_size));
        if (!read_pod(in, offset)) {
            error = "invalid delta row";
            return false;
        }
        entries_[key] = offset;
    }
    return true;
}

bool BTreeIndex::load_legacy_index(const std::string &path, std::string &error) {
    std::ifstream in(path);
    if (!in) {
        return true;
    }
    std::string line;
    while (std::getline(in, line)) {
        auto parts = split(line, '|');
        if (parts.size() != 2) {
            error = "invalid index row";
            return false;
        }
        entries_[parts[0]] = static_cast<std::uint64_t>(std::stoull(parts[1]));
    }
    return true;
}

bool BTreeIndex::save_snapshot(const std::string &path, std::string &error) const {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error = "failed to write index snapshot";
        return false;
    }
    const std::uint64_t count = static_cast<std::uint64_t>(entries_.size());
    if (!write_pod(out, kSnapshotMagic) || !write_pod(out, count)) {
        error = "failed to write index snapshot";
        return false;
    }
    for (const auto &[key, offset] : entries_) {
        const std::uint32_t key_size = static_cast<std::uint32_t>(key.size());
        if (!write_pod(out, key_size)) {
            error = "failed to write index snapshot";
            return false;
        }
        out.write(key.data(), static_cast<std::streamsize>(key.size()));
        if (!write_pod(out, offset)) {
            error = "failed to write index snapshot";
            return false;
        }
    }
    return static_cast<bool>(out);
}

bool BTreeIndex::append_delta(
    const std::string &path,
    const std::vector<std::pair<std::string, std::uint64_t>> &updates,
    std::string &error) const {
    const bool write_header = !std::filesystem::exists(path) || std::filesystem::file_size(path) == 0;
    std::ofstream out(path, std::ios::binary | std::ios::app);
    if (!out) {
        error = "failed to append delta log";
        return false;
    }
    if (write_header && !write_pod(out, kDeltaMagic)) {
        error = "failed to append delta log";
        return false;
    }
    for (const auto &[key, offset] : updates) {
        const std::uint32_t key_size = static_cast<std::uint32_t>(key.size());
        if (!write_pod(out, key_size)) {
            error = "failed to append delta log";
            return false;
        }
        out.write(key.data(), static_cast<std::streamsize>(key.size()));
        if (!write_pod(out, offset)) {
            error = "failed to append delta log";
            return false;
        }
    }
    return static_cast<bool>(out);
}

void BTreeIndex::rebuild_bloom_filter() {
    std::size_t bit_count = std::max<std::size_t>(8192, entries_.size() * 16);
    bloom_filter_.reset(bit_count, 3);
    for (const auto &[key, _] : entries_) {
        bloom_filter_.add(key);
    }
}

}  // namespace flexql
