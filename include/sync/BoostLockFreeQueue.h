//
// Created by 杜建璋 on 2025/1/19.
//

#ifndef LOCKFREEBLOCKINGQUEUE_H
#define LOCKFREEBLOCKINGQUEUE_H

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <condition_variable>

#include "AbstractBlockingQueue.h"

template<typename T>
class BoostLockFreeQueue : public AbstractBlockingQueue<T> {
private:
    boost::lockfree::queue<T, boost::lockfree::fixed_sized<true>> queue;
    std::atomic_size_t _size;
    size_t max_capacity;

public:
    explicit BoostLockFreeQueue(size_t capacity)
        : queue(capacity), max_capacity(capacity), _size(0) {}

    void offer(const T& item) override {
        while (!queue.push(item)) {}
        ++_size;
    }

    T poll() override {
        T item;
        while (!queue.pop(item)) {}
        --_size;
        return item;
    }

    [[nodiscard]] size_t size() const override {
        return _size;
    }

    [[nodiscard]] size_t capacity() const override {
        return max_capacity;
    }
};


#endif //LOCKFREEBLOCKINGQUEUE_H
