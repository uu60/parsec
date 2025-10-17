
#ifndef CTPLTHREADPOOL_H
#define CTPLTHREADPOOL_H
#include "../third_party/ctpl_stl.h"
#include "../utils/Log.h"

class CtplThreadPool {
private:
    ctpl::thread_pool *pool;

public:
    explicit CtplThreadPool(int num_threads) {
        pool = new ctpl::thread_pool(num_threads);
    }

    ~CtplThreadPool() {
        delete pool;
    }

    template<typename F>
    auto submit(F &&f) -> std::future<decltype(f())> {
        using ReturnType = std::invoke_result_t<F>;

        auto res = pool->push([f = std::forward<F>(f)](int) -> ReturnType {
            return f();
        });
        return res;
    }
};


#endif
