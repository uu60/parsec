//
// Created by 杜建璋 on 2024/7/26.
//

#include "utils/System.h"

folly::IOThreadPoolExecutor System::_threadPool(std::thread::hardware_concurrency() * 8);

int64_t System::currentTimeMillis()  {
    auto now = std::chrono::system_clock::now();

    auto duration = now.time_since_epoch();
    long millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    return millis;
}
