//
// Created by 杜建璋 on 2024/12/8.
//

#ifndef BLOCKINGQUEUE_H
#define BLOCKINGQUEUE_H
#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>

#include "AbstractBlockingQueue.h"
#include "../utils/Log.h"

template<typename T>
class LockBlockingQueue : public AbstractBlockingQueue<T> {
private:
    std::queue<T> _queue;
    std::mutex _mutex;
    std::condition_variable _notEmpty;
    std::condition_variable _notFull;
    size_t _maxSize;

public:
    explicit LockBlockingQueue(size_t max_size) : _maxSize(max_size) {
    }

    void offer(T item) override {
        std::unique_lock<std::mutex> lock(_mutex);
        _notFull.wait(lock, [this]() { return _queue.size() < _maxSize; });
        _queue.push(item);
        _notEmpty.notify_one();
    }

    T poll() override {
        std::unique_lock<std::mutex> lock(_mutex);
        _notEmpty.wait(lock, [this]() { return !_queue.empty(); });
        T item = _queue.front();
        _queue.pop();
        _notFull.notify_one();
        return item;
    }

    [[nodiscard]] size_t size() const override {
        return _queue.size();
    }

    [[nodiscard]] size_t capacity() const override {
        return _maxSize;
    }
};
#endif //BLOCKINGQUEUE_H
