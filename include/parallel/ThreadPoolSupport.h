//
// Created by 杜建璋 on 2025/2/13.
//

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <future>

#include "./CtplThreadPool.h"
#include "./TbbThreadPool.h"
#include "Async.h"
#include "../conf/Conf.h"


class ThreadPoolSupport {
public:
    inline static CtplThreadPool *_ctplPool = nullptr;
    inline static TbbThreadPool *_tbbPool = nullptr;
    inline static Async *_async = nullptr;
    // inline static std::atomic_int _availableThreads = Conf::LOCAL_THREADS;

public:
    static void init() {
        if (Conf::DISABLE_MULTI_THREAD) {
            return;
        }
        if (Conf::THREAD_POOL_TYPE == Conf::CTPL_POOL) {
            _ctplPool = new CtplThreadPool(Conf::LOCAL_THREADS);
        } else if (Conf::THREAD_POOL_TYPE == Conf::TBB_POOL) {
            _tbbPool = new TbbThreadPool(Conf::LOCAL_THREADS);
        } else if (Conf::THREAD_POOL_TYPE == Conf::ASYNC) {
            _async = new Async();
        }
    }

    static void finalize() {
        delete _ctplPool;
        delete _tbbPool;
        delete _async;
    }

    template <typename F>
    static std::future<std::invoke_result_t<F>> callerRun(F &&f) {
        // If no proper thread pools, execute on current thread.
        using ReturnType = std::invoke_result_t<F>;
        std::promise<ReturnType> promise;
        if constexpr (std::is_void_v<ReturnType>) {
            std::invoke(std::forward<F>(f));
            promise.set_value();
        } else {
            promise.set_value(std::invoke(std::forward<F>(f)));
        }
        return promise.get_future();
    }

    template<typename F>
    static auto submit(F &&f) -> std::future<std::invoke_result_t<F> > {
        if (Conf::THREAD_POOL_TYPE == Conf::CTPL_POOL) {
            return _ctplPool->submit(f);
        }
        if (Conf::THREAD_POOL_TYPE == Conf::TBB_POOL) {
            return _tbbPool->submit(f);
        }
        if (Conf::THREAD_POOL_TYPE == Conf::ASYNC) {
            return _async->submit(f);
        }
        // If no proper pool, run in caller itself
        return callerRun(f);
    }
};


#endif //THREADPOOL_H
