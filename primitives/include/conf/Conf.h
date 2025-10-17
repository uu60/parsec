
#ifndef CONF_H
#define CONF_H
#include <thread>
#include <climits>
#include <map>

#include <string>

class Conf {
public:
    inline static std::map<std::string, std::string> _userParams{};

    enum PoolT {
        CTPL_POOL,
#ifdef PARSEC_HAS_TBB
        TBB_POOL,
#endif
        ASYNC,
    };

    enum QueueT {
        LOCK_FREE_QUEUE,
        LOCK_QUEUE,
        SPSC_QUEUE,
    };

    enum CommT {
        MPI
    };

    enum BmtT {
        BMT_BACKGROUND,
        BMT_JIT,
        BMT_FIXED,
        BMT_PIPELINE
    };

public:
    static void init(int argc, char **argv);

    inline static BmtT BMT_METHOD = BMT_JIT;
    inline static int BMT_PRE_GEN_SECONDS = 0;
    inline static int MAX_BMTS = INT_MAX;
    inline static int BMT_USAGE_LIMIT = 1;
    inline static QueueT BMT_QUEUE_TYPE = SPSC_QUEUE;
    inline static int BMT_QUEUE_NUM = 1;
    inline static bool DISABLE_ARITH = true;
    inline static int BMT_GEN_BATCH_SIZE = 10000;

    inline static int TASK_TAG_BITS = 6;
    inline static bool DISABLE_MULTI_THREAD = false;
    inline static bool ENABLE_INTRA_OPERATOR_PARALLELISM = false;
    inline static int LOCAL_THREADS = static_cast<int>(std::thread::hardware_concurrency() * 100);
    inline static int THREAD_POOL_TYPE = ASYNC;

    inline static CommT COMM_TYPE = MPI;
    inline static int BATCH_SIZE = 1000;
    inline static bool ENABLE_TRANSFER_COMPRESSION = false;
    inline static bool ENABLE_REDUNDANT_OT = true;

    inline static bool ENABLE_CLASS_WISE_TIMING = false;

    inline static bool ENABLE_SIMD = true;
};


#endif
