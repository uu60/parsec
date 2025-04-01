//
// Created by 杜建璋 on 2025/1/19.
//

#ifndef CONF_H
#define CONF_H
#include <thread>

class Conf {
public:
    // pool type
    enum PoolT {
        CTPL_POOL,
        TBB_POOL
    };

    // blocking queue type
    enum QueueT {
        CAS_QUEUE,
        LOCK_QUEUE
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
        BMT_BATCH_BACKGROUND
     };
public:
    static void init(int argc, char **argv);

    // ---------------Settings for bmts---------------
    // If intermediate data produced in background (DO NOT FORGET TO ENABLE MULTI-THREAD)
    inline static BmtT BMT_METHOD = BMT_JIT;
    // Bmt max num in queue (INVALID when BMT_BACKGROUND is false)
    inline static int MAX_BMTS = 10000;
    // Used times limit of one bmt (INVALID when BMT_BACKGROUND is false)
    inline static int BMT_USAGE_LIMIT = 1;
    // Blocking Bmt Queue (INVALID when background bmt is disabled)
    inline static QueueT BMT_QUEUE_TYPE = CAS_QUEUE;

    // ---------------Settings for threads---------------
    // Task tag bits
    inline static int TASK_TAG_BITS = 3;
    // Enable single-thread only
    inline static bool DISABLE_MULTI_THREAD = true;
    // Enable multiple-thread computation in each single executor
    inline static bool INTRA_OPERATOR_PARALLELISM = false;
    // Sum of threads in a process
    inline static int LOCAL_THREADS = static_cast<int>(std::thread::hardware_concurrency());
    // Index of thread pool type (0 = ctpl, 1 = tbb)
    inline static int THREAD_POOL_TYPE = CTPL_POOL;

    // ---------------Settings for networks---------------
    // Communication object index (0 = OpenMpi)
    inline static CommT COMM_TYPE = MPI;
    // Invalid if intra parallelism or batching is false
    inline static int BATCH_SIZE = 10;
    // Transfer compression
    inline static bool ENABLE_TRANSFER_COMPRESSION = true;

    // ---------------Settings for benchmark---------------
    inline static bool ENABLE_CLASS_WISE_TIMING = false;

    // ---------------Settings for sort---------------
    // Sort in parallel
    inline static bool SORT_IN_PARALLEL = false;
    // Max sorting threads
    inline static int MAX_SORTING_THREADS = 4;
    // Sort method
    inline static bool SORT_IN_RECURSIVE = false;

    // ---------------Settings for acceleration---------------
    inline static bool ENABLE_SIMD = true;
};


#endif //CONF_H
