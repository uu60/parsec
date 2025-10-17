
#ifndef MPC_PACKAGE_SYSTEM_H
#define MPC_PACKAGE_SYSTEM_H
#include "../third_party/ctpl_stl.h"
#include "../comm/Comm.h"
#include "../conf/Conf.h"

class System {
private:
    
    inline static int PRESERVED_TASK_TAGS = 2 * Conf::BMT_QUEUE_NUM;
    inline static std::atomic_int _currentTaskTag = PRESERVED_TASK_TAGS;

public:
    inline static std::atomic_bool _shutdown = false;

public:
    static void init(int argc, char **argv);

    static void finalize();

    static int nextTask();

    static int64_t currentTimeMillis();
};


#endif
