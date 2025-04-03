//
// Created by 杜建璋 on 2025/4/3.
//

#ifndef CPPASYNCTHREADPOOL_H
#define CPPASYNCTHREADPOOL_H
#include <future>


class CppAsyncThreadPool {
public:
    template<typename F>
    auto submit(F &&f) -> std::future<decltype(f())> {
        return std::async(std::launch::async, [f = std::forward<F>(f)] {
            return f();
        });
    }
};


#endif //CPPASYNCTHREADPOOL_H
