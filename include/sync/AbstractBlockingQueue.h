//
// Created by 杜建璋 on 2025/1/18.
//

#ifndef ABSTRACTBLOCKINGQUEUE_H
#define ABSTRACTBLOCKINGQUEUE_H

template <typename T>
class AbstractBlockingQueue {
public:
    virtual ~AbstractBlockingQueue() = default;

    virtual void offer(T item) = 0;

    virtual T poll() = 0;

    [[nodiscard]] virtual size_t size() const = 0;

    [[nodiscard]] virtual size_t capacity() const = 0;
};


#endif //ABSTRACTBLOCKINGQUEUE_H
