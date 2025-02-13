//
// Created by 杜建璋 on 2024/7/26.
//

#ifndef MPC_PACKAGE_SYSTEM_H
#define MPC_PACKAGE_SYSTEM_H
#include "../third_party/ctpl_stl.h"
#include "../comm/Comm.h"

class System {
private:
    /**
     * 0 is preserved for BmtGenerator.
     * 1 is preserved for BitwiseBmtGenerator.
     * 2 is preserved for BaseOtExecutor.
     * 3 is preserved for ABPairGenerator.
     */
    static constexpr int16_t PRESERVED_TASK_TAGS = 4;
    inline static std::atomic_int16_t _currentTaskTag = PRESERVED_TASK_TAGS;

public:
    static std::atomic_bool _shutdown;
    static ctpl::thread_pool _threadPool;

    static void init(Comm *impl, int argc, char **argv);

    static void finalize();

    static int16_t nextTask();

    static int64_t currentTimeMillis();
};


#endif //MPC_PACKAGE_SYSTEM_H
