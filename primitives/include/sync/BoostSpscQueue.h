
#ifndef BOOSTSPSCQUEUE_H
#define BOOSTSPSCQUEUE_H


#include <boost/lockfree/spsc_queue.hpp>
#include <thread>
#include "utils/Log.h"

#include "AbstractBlockingQueue.h"

template <typename T, size_t C>
class BoostSPSCQueue : public AbstractBlockingQueue<T> {
private:
    boost::lockfree::spsc_queue<T, boost::lockfree::capacity<C>> _queue;

public:
    BoostSPSCQueue() : _queue() {}

    void offer(T item) override {
        while (!_queue.push(item)) {
            std::this_thread::yield();
        }
    }

    T poll() override {
        T item;
        while (!_queue.pop(item)) {
            std::this_thread::yield();
        }
        return item;
    }

    [[nodiscard]] size_t size() const override {
        return -1;
    }

    [[nodiscard]] size_t capacity() const override {
        return C;
    }
};



#endif
