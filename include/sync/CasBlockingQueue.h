//
// Created by 杜建璋 on 2025/1/19.
//

#ifndef LOCKFREEBLOCKINGQUEUE_H
#define LOCKFREEBLOCKINGQUEUE_H

#include <atomic>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <iostream>

#include "BlockingQueue.h"

template<typename T>
class CasBlockingQueue : public BlockingQueue<T> {
public:
    explicit CasBlockingQueue(size_t capacity)
        : _capacity(capacity), _head(0), _tail(0), _size(0) {
        _buffer.resize(capacity);
    }

    void offer(const T &item) override {
        while (true) {
            size_t tail = _tail.load(std::memory_order_relaxed);
            size_t size = _size.load(std::memory_order_acquire);

            if (size >= _capacity) {
                std::unique_lock<std::mutex> lock(mutex_);
                not_full_cv_.wait(lock, [this]() {
                    return _size.load(std::memory_order_acquire) < _capacity;
                });
            }

            if (_tail.compare_exchange_weak(tail, (tail + 1) % _capacity, std::memory_order_acq_rel)) {
                _buffer[tail] = item;
                _size.fetch_add(1, std::memory_order_release);
                not_empty_cv_.notify_one();
                return;
            }
        }
    }

    T poll() override {
        while (true) {
            size_t head = _head.load(std::memory_order_relaxed);
            size_t size = _size.load(std::memory_order_acquire);

            if (size == 0) {
                std::unique_lock<std::mutex> lock(mutex_);
                not_empty_cv_.wait(lock, [this]() {
                    return _size.load(std::memory_order_acquire) > 0;
                });
            }

            if (_head.compare_exchange_weak(head, (head + 1) % _capacity, std::memory_order_acq_rel)) {
                T item = std::move(_buffer[head]);
                _size.fetch_sub(1, std::memory_order_release);
                not_full_cv_.notify_one();
                return item;
            }
        }
    }

    size_t size() const override {
        return _size.load(std::memory_order_acquire);
    }

    // 返回队列的最大容量
    size_t capacity() const override {
        return _capacity;
    }

private:
    const size_t _capacity;
    std::vector<T> _buffer;
    std::atomic<size_t> _head, _tail, _size;
    std::mutex mutex_;
    std::condition_variable not_empty_cv_, not_full_cv_;
};


#endif //LOCKFREEBLOCKINGQUEUE_H
