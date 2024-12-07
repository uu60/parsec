//
// Created by 杜建璋 on 2024/7/26.
//

#ifndef MPC_PACKAGE_SYSTEM_H
#define MPC_PACKAGE_SYSTEM_H
#include <folly/executors/IOThreadPoolExecutor.h>

class System {
public:
    static folly::IOThreadPoolExecutor _threadPool;

    static int64_t currentTimeMillis();
};


#endif //MPC_PACKAGE_SYSTEM_H
