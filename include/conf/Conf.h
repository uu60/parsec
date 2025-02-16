//
// Created by 杜建璋 on 2025/1/19.
//

#ifndef CONF_H
#define CONF_H
#include <thread>

#include "../comm/MpiComm.h"
#include "../intermediate/item/Bmt.h"
#include "../sync/AbstractBlockingQueue.h"
#include "../sync/CasBlockingQueue.h"
#include "../sync/LockBlockingQueue.h"
#include "../intermediate/item/BitwiseBmt.h"

class Conf {
public:
    // ---------------Settings for bmts---------------
    // If intermediate data produced in background
    inline static bool BMT_BACKGROUND = false;
    // Bmt max num in queue (INVALID when BMT_BACKGROUND is false)
    inline static int MAX_BMTS = 10000;
    // Used times limit of one bmt (INVALID when BMT_BACKGROUND is false)
    inline static int BMT_USAGE_LIMIT = 1;
    // Blocking Bmt Queue
    inline static AbstractBlockingQueue<Bmt> *BMT_QUEUE = new CasBlockingQueue<Bmt>(MAX_BMTS);
    // Blocking Bitwise Bmt Queue
    inline static AbstractBlockingQueue<BitwiseBmt> *BITWISE_BMT_QUEUE = new CasBlockingQueue<BitwiseBmt>(MAX_BMTS);

    // ---------------Settings for threads---------------
    // Enable multiple-thread computation in each single executor
    inline static bool INTRA_OPERATOR_PARALLELISM = true;
    // Sum of threads in a process
    inline static int LOCAL_THREADS = static_cast<int>(std::thread::hardware_concurrency() * 100);
    // Separate thread groups for computation and networks
    inline static bool THREAD_SEPARATION = false;
    // Thread number of computing threads (INVALID when THREAD_SEPARATION is false)
    inline static int COMPUTING_THREADS = static_cast<int>(std::thread::hardware_concurrency());
    // Thread number of network threads (INVALID when THREAD_SEPARATION is false)
    inline static int NETWORK_THREADS = static_cast<int>(std::thread::hardware_concurrency());
    // Thread pool task queue separation
    inline static bool JOB_QUEUE_SEPARATION = false;

    // ---------------Settings for networks---------------
    // Communication object
    inline static Comm *COMM_IMPL = new MpiComm;
    // Batch communicate or execute by elements
    inline static bool BATCH_COMM = true;

    // ---------------Settings for benchmark---------------
    inline static bool CLASS_WISE_TIMING = true;
};



#endif //CONF_H
