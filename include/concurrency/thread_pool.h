#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace flexql {

class ThreadPool {
public:
    explicit ThreadPool(std::size_t worker_count = 0);
    ~ThreadPool();

    void enqueue(std::function<void()> work_item);
    void shutdown();

private:
    void worker_loop();

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
};

}  // namespace flexql
