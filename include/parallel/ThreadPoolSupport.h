//
// Created by 杜建璋 on 2025/2/13.
//

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <future>

#include "./CtplThreadPool.h"
#include "./TbbThreadPool.h"
#include "../conf/Conf.h"


class ThreadPoolSupport {
public:
    inline static CtplThreadPool *_ctplPool = nullptr;
    inline static TbbThreadPool *_tbbPool = nullptr;

public:
    static void init() {
        if (Conf::THREAD_POOL_TYPE == Consts::CTPL_POOL) {
            _ctplPool = new CtplThreadPool(Conf::LOCAL_THREADS);
        } else if (Conf::THREAD_POOL_TYPE == Consts::TBB_POOL) {
            _tbbPool = new TbbThreadPool(Conf::LOCAL_THREADS);
        }
    }

    template<typename F>
    static auto submit(F &&f) -> std::future<std::invoke_result_t<F> > {
        if constexpr (Conf::THREAD_POOL_TYPE == Consts::CTPL_POOL) {
            return _ctplPool->submit(f);
        }
        if constexpr (Conf::THREAD_POOL_TYPE == Consts::TBB_POOL) {
            return _tbbPool->submit(f);
        }
        // If no proper thread pools, execute on current thread.
        using ReturnType = std::invoke_result_t<F>;
        std::promise<ReturnType> promise;
        if constexpr (std::is_void_v<ReturnType>) {
            std::invoke(std::forward<F>(f));
            promise.set_value();
        } else {
            promise.set_value(std::invoke(std::forward<F>(f)));
        }
    }
};


#endif //THREADPOOL_H
