
#ifndef ABSTRACTBLOCKINGQUEUE_H
#define ABSTRACTBLOCKINGQUEUE_H

namespace parsec {


template <typename T>
class AbstractBlockingQueue {
public:
    virtual ~AbstractBlockingQueue() = default;

    virtual void offer(T item) = 0;

    virtual T poll() = 0;

    [[nodiscard]] virtual size_t size() const = 0;

    [[nodiscard]] virtual size_t capacity() const = 0;
};



} // namespace parsec

#endif
