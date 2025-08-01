//
// Created by 杜建璋 on 2025/2/20.
//

#ifndef TBBTHREADPOOL_H
#define TBBTHREADPOOL_H

#ifdef PARSEC_HAS_TBB
#include <future>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>

class TbbThreadPool {
private:
    tbb::task_arena arena;
    tbb::task_group group;

public:
    explicit TbbThreadPool(int num_threads) : arena(num_threads) {
    }

    template<typename F>
    auto submit(F &&f) -> std::future<decltype(f())> {
        using ReturnType = decltype(f());
        auto task = std::make_shared<std::packaged_task<ReturnType()> >(
            [f = std::forward<F>(f)]() -> ReturnType {
                return f();
            }
        );

        std::future<ReturnType> future = task->get_future();

        arena.execute([this, task]() {
            group.run([task]() { (*task)(); });
        });

        return future;
    }
};

#else
// Dummy TbbThreadPool class when TBB is not available
#include <future>
#include <stdexcept>

class TbbThreadPool {
public:
    explicit TbbThreadPool(int num_threads) {
        // Do nothing when TBB is not available
    }

    template<typename F>
    auto submit(F &&f) -> std::future<decltype(f())> {
        // This should never be called when TBB is not available
        // The ThreadPoolSupport should handle this case
        throw std::runtime_error("TBB thread pool is not available");
    }
};

#endif // PARSEC_HAS_TBB

#endif //TBBTHREADPOOL_H
