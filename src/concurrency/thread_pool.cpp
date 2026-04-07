#include "concurrency/thread_pool.h"

#include <algorithm>

namespace flexql {

ThreadPool::ThreadPool(std::size_t requested_worker_count) {
    std::size_t worker_total = requested_worker_count;
    if (worker_total == 0) {
        worker_total = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    }

    workers_.reserve(worker_total);
    for (std::size_t worker_index = 0; worker_index < worker_total; ++worker_index) {
        workers_.emplace_back([this]() { worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::enqueue(std::function<void()> queued_task) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (stopping_) {
            return;
        }

        tasks_.push(std::move(queued_task));
    }
    cv_.notify_one();
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (stopping_) {
            return;
        }

        stopping_ = true;
    }

    cv_.notify_all();
    for (auto &thread_handle : workers_) {
        if (thread_handle.joinable()) {
            thread_handle.join();
        }
    }
}

void ThreadPool::worker_loop() {
    for (;;) {
        std::function<void()> next_task;
        {
            std::unique_lock<std::mutex> wait_lock(mutex_);
            cv_.wait(wait_lock, [this]() { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) {
                return;
            }

            next_task = std::move(tasks_.front());
            tasks_.pop();
        }

        next_task();
    }
}

}  // namespace flexql
