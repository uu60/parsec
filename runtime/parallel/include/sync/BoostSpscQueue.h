
#ifndef BOOSTSPSCQUEUE_H
#define BOOSTSPSCQUEUE_H

#include <atomic>
#include <boost/lockfree/spsc_queue.hpp>
#include <stdexcept>
#include <thread>

#include "AbstractBlockingQueue.h"

template <typename T>
class BoostSPSCQueue : public AbstractBlockingQueue<T> {
private:
    boost::lockfree::spsc_queue<T> _queue;
    const size_t _capacity;
    std::atomic<bool> _closed{false};

public:
    explicit BoostSPSCQueue(size_t capacity) : _queue(capacity), _capacity(capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("BoostSPSCQueue capacity must be greater than zero");
        }
    }

    void offer(T item) override {
        while (!_closed.load(std::memory_order_acquire)) {
            if (_queue.push(item)) {
                return;
            }
            std::this_thread::yield();
        }
    }

    T poll() override {
        T item;
        while (!_queue.pop(item)) {
            if (_closed.load(std::memory_order_acquire)) {
                throw std::runtime_error("BoostSPSCQueue closed while waiting for an item");
            }
            std::this_thread::yield();
        }
        return item;
    }

    void close() override {
        _closed.store(true, std::memory_order_release);
    }

    [[nodiscard]] size_t size() const override {
        return _queue.read_available();
    }

    [[nodiscard]] size_t capacity() const override {
        return _capacity;
    }
};



#endif
