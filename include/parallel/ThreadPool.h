//
// Created by 杜建璋 on 2025/2/13.
//

#ifndef THREADPOOL_H
#define THREADPOOL_H



#pragma once

#include <future>
#include <functional>

class ThreadPool {
// public:
//     virtual ~ThreadPool() = default;
//
//     template <typename F, typename... Args>
//     auto submit(F&& func, Args&&... args)
//         -> std::future<typename std::invoke_result_t<F, Args...>>
//     {
//         using ReturnType = std::invoke_result_t<F, Args...>;
//         return submit<ReturnType>(std::forward<F>(func), std::forward<Args>(args)...);
//     }
//
//     virtual void wait() = 0;
//
// protected:
//     template <typename T, typename F, typename... Args>
//     virtual std::future<T> push(F&& func, Args&&... args) = 0;
};



#endif //THREADPOOL_H
