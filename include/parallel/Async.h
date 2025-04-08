//
// Created by 杜建璋 on 2025/4/3.
//

#ifndef CPPASYNCTHREADPOOL_H
#define CPPASYNCTHREADPOOL_H
#include <future>


class Async {
public:
    template<typename F>
    auto submit(F &&f) -> std::future<decltype(f())> {
        using ReturnType = decltype(f());

        std::promise<ReturnType> promise;
        std::future<ReturnType> fut = promise.get_future();

        std::thread([p = std::move(promise), func = std::forward<F>(f)]() mutable {
            try {
                if constexpr (std::is_void_v<ReturnType>) {
                  func();
                  p.set_value();
              } else {
                  p.set_value(func());
              }
            } catch (...) {
                p.set_exception(std::current_exception());
            }
        }).detach();

        return fut;
    }
};


#endif //CPPASYNCTHREADPOOL_H
