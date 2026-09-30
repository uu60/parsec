
#ifndef LOCKFREEBLOCKINGQUEUE_H
#define LOCKFREEBLOCKINGQUEUE_H

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <stdexcept>
#include <thread>

#include "../sync/AbstractBlockingQueue.h"

template<typename T>
class BoostLockFreeQueue : public AbstractBlockingQueue<T> {
private:
    boost::lockfree::queue<T, boost::lockfree::fixed_sized<true>> queue;
    size_t max_capacity;
    std::atomic<bool> closed{false};

public:
    explicit BoostLockFreeQueue(size_t capacity)
        : queue(capacity), max_capacity(capacity) {}

    void offer(T item) override {
        while (!closed.load(std::memory_order_acquire)) {
            if (queue.push(item)) {
                return;
            }
            std::this_thread::yield();
        }
    }

    T poll() override {
        T item;
        while (!queue.pop(item)) {
            if (closed.load(std::memory_order_acquire)) {
                throw std::runtime_error("BoostLockFreeQueue closed while waiting for an item");
            }
            std::this_thread::yield();
        }
        return item;
    }

    void close() override {
        closed.store(true, std::memory_order_release);
    }

    [[nodiscard]] size_t size() const override {
        return -1;
    }

    [[nodiscard]] size_t capacity() const override {
        return max_capacity;
    }
};


#endif
