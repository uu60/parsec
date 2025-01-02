//
// Created by 杜建璋 on 2024/12/8.
//

#ifndef BLOCKINGQUEUE_H
#define BLOCKINGQUEUE_H
#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <optional>

#include "../utils/Log.h"

template<typename T>
class BlockingQueue {
private:
    std::queue<T> _queue;
    std::mutex _mutex;
    std::condition_variable _notEmpty;
    std::condition_variable _notFull;
    size_t _maxSize;

public:
    explicit BlockingQueue(size_t max_size) : _maxSize(max_size) {
    }

    void push(const T &item) {
        std::unique_lock<std::mutex> lock(_mutex);
        _notFull.wait(lock, [this]() { return _queue.size() < _maxSize; });
        _queue.push(item);
        _notEmpty.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(_mutex);
        _notEmpty.wait(lock, [this]() { return !_queue.empty(); });
        T item = _queue.front();
        _queue.pop();
        _notFull.notify_one();
        return item;
    }

    std::optional<T> try_pop() {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_queue.empty()) {
            return std::nullopt;
        }
        T item = _queue.front();
        _queue.pop();
        _notFull.notify_one();
        return item;
    }

    size_t size() {
        return _queue.size();
    }
};
#endif //BLOCKINGQUEUE_H
