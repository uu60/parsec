//
// Created by 杜建璋 on 2024/7/26.
//

#ifndef MPC_PACKAGE_SYSTEM_H
#define MPC_PACKAGE_SYSTEM_H
#include "../third_party/ctpl_stl.h"
#include "../comm/IComm.h"

class System {
private:
    /**
     * 0 is preserved for OtBmtGenerator.
     * 1 is preserved for ABPairGenerator.
     * 2 is preserved for BaseOtExecutor
     */
    static constexpr int16_t _preservedTaskTags = 3;
    inline static std::atomic_int16_t _currentTaskTag = _preservedTaskTags;

public:
    static std::atomic_bool _shutdown;
    static ctpl::thread_pool _threadPool;

    static void init(IComm *impl, int argc, char **argv);

    static void finalize();

    static int16_t nextTask();

    static int64_t currentTimeMillis();
};


#endif //MPC_PACKAGE_SYSTEM_H
