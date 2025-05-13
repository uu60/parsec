//
// Created by 杜建璋 on 2025/1/19.
//

#ifndef CONF_H
#define CONF_H
#include <thread>
#include <climits>
#include <map>

class Conf {
public:
    inline static std::map<std::string,std::string> _userParams{};

    // pool type
    enum PoolT {
        CTPL_POOL,
        TBB_POOL,
        ASYNC,
    };

    // blocking queue type
    enum QueueT {
        LOCK_FREE_QUEUE,
        LOCK_QUEUE,
        SPSC_QUEUE,
    };

    // comm type
    enum CommT {
        MPI
    };

    // bmt generation
    enum BmtT {
        BMT_BACKGROUND,
        BMT_JIT,
        BMT_FIXED,
        BMT_PIPELINE
     };
public:
    static void init(int argc, char **argv);

    // ---------------Settings for bmts---------------
    // If intermediate data produced in background (DO NOT FORGET TO ENABLE MULTI-THREAD)
    inline static BmtT BMT_METHOD = BMT_JIT;
    // Pregen time
    inline static int BMT_PRE_GEN_SECONDS = 0;
    // Bmt max num in queue (INVALID when BMT_BACKGROUND is false)
    inline static int MAX_BMTS = INT_MAX;
    // Used times limit of one bmt (INVALID when BMT_BACKGROUND is false)
    inline static int BMT_USAGE_LIMIT = 1;
    // Blocking Bmt Queue (INVALID when background bmt is disabled)
    inline static QueueT BMT_QUEUE_TYPE = SPSC_QUEUE;
    // Bmt Queue num
    inline static int BMT_QUEUE_NUM = 1;
    // Disable arith algorithms
    inline static bool DISABLE_ARITH = true;
    // Bmt gen batch size
    inline static int BMT_GEN_BATCH_SIZE = 100000;

    // ---------------Settings for threads---------------
    // Task tag bits
    inline static int TASK_TAG_BITS = 6;
    // Enable single-thread only
    inline static bool DISABLE_MULTI_THREAD = true;
    // Enable multiple-thread computation in each single executor
    inline static bool ENABLE_INTRA_OPERATOR_PARALLELISM = false;
    // Sum of threads in a process
    inline static int LOCAL_THREADS = static_cast<int>(std::thread::hardware_concurrency() * 10);
    // Index of thread pool type (0 = ctpl, 1 = tbb)
    inline static int THREAD_POOL_TYPE = ASYNC;

    // ---------------Settings for networks---------------
    // Communication object index (0 = OpenMpi)
    inline static CommT COMM_TYPE = MPI;
    // Invalid if intra parallelism or batching is false
    inline static int BATCH_SIZE = 100000;
    // Transfer compression
    inline static bool ENABLE_TRANSFER_COMPRESSION = false;
    // Random OT in redundant data transfer
    inline static bool ENABLE_REDUNDANT_OT = true;

    // ---------------Settings for benchmark---------------
    inline static bool ENABLE_CLASS_WISE_TIMING = false;

    // ---------------Settings for acceleration---------------
    inline static bool ENABLE_SIMD = true;
};


#endif //CONF_H
