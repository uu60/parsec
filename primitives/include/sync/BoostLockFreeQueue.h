
#ifndef LOCKFREEBLOCKINGQUEUE_H
#define LOCKFREEBLOCKINGQUEUE_H

#include <thread>
#include <boost/lockfree/queue.hpp>

#include "../sync/AbstractBlockingQueue.h"

template<typename T>
class BoostLockFreeQueue : public AbstractBlockingQueue<T> {
private:
    boost::lockfree::queue<T, boost::lockfree::fixed_sized<true>> queue;
    size_t max_capacity;

public:
    explicit BoostLockFreeQueue(size_t capacity)
        : queue(capacity), max_capacity(capacity) {}

    void offer(T item) override {
        while (!queue.push(item)) {
            std::this_thread::yield();
        }
    }

    T poll() override {
        T item;
        while (!queue.pop(item)) {
            std::this_thread::yield();
        }
        return item;
    }

    [[nodiscard]] size_t size() const override {
        return -1;
    }

    [[nodiscard]] size_t capacity() const override {
        return max_capacity;
    }
};


#endif
