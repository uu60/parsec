
#ifndef ABSTRACTBLOCKINGQUEUE_H
#define ABSTRACTBLOCKINGQUEUE_H

template <typename T>
class AbstractBlockingQueue {
public:
    virtual ~AbstractBlockingQueue() = default;

    virtual void offer(T item) = 0;

    virtual T poll() = 0;

    // Unblock producers/consumers during shutdown. Calls after close either
    // return without enqueueing or throw when no queued item remains.
    virtual void close() = 0;

    [[nodiscard]] virtual size_t size() const = 0;

    [[nodiscard]] virtual size_t capacity() const = 0;
};


#endif
