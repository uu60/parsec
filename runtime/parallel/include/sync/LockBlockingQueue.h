
#ifndef BLOCKINGQUEUE_H
#define BLOCKINGQUEUE_H
#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

#include "AbstractBlockingQueue.h"
#include "utils/Log.h"

template<typename T>
class LockBlockingQueue : public AbstractBlockingQueue<T> {
private:
    std::queue<T> _queue;
    mutable std::mutex _mutex;
    std::condition_variable _notEmpty;
    std::condition_variable _notFull;
    size_t _maxSize;
    bool _closed{false};

public:
    explicit LockBlockingQueue(size_t max_size) : _maxSize(max_size) {
    }

    void offer(T item) override {
        std::unique_lock<std::mutex> lock(_mutex);
        _notFull.wait(lock, [this]() { return _closed || _queue.size() < _maxSize; });
        if (_closed) {
            return;
        }
        _queue.push(item);
        _notEmpty.notify_one();
    }

    T poll() override {
        std::unique_lock<std::mutex> lock(_mutex);
        _notEmpty.wait(lock, [this]() { return _closed || !_queue.empty(); });
        if (_queue.empty()) {
            throw std::runtime_error("LockBlockingQueue closed while waiting for an item");
        }
        T item = _queue.front();
        _queue.pop();
        _notFull.notify_one();
        return item;
    }

    void close() override {
        std::lock_guard<std::mutex> lock(_mutex);
        _closed = true;
        _notEmpty.notify_all();
        _notFull.notify_all();
    }

    [[nodiscard]] size_t size() const override {
        std::lock_guard<std::mutex> lock(_mutex);
        return _queue.size();
    }

    [[nodiscard]] size_t capacity() const override {
        return _maxSize;
    }
};
#endif
