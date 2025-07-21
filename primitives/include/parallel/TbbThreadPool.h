//
// Created by 杜建璋 on 2025/2/20.
//

#ifndef TBBTHREADPOOL_H
#define TBBTHREADPOOL_H
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


#endif //TBBTHREADPOOL_H
