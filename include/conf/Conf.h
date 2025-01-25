//
// Created by 杜建璋 on 2025/1/19.
//

#ifndef CONF_H
#define CONF_H
#include <thread>

#include "../comm/MpiComm.h"
#include "../intermediate/item/Bmt.h"
#include "../sync/BlockingQueue.h"
#include "../sync/CasBlockingQueue.h"
#include "../sync/LockBlockingQueue.h"

class Conf {
public:
    // If intermediate data produced in background
    inline static bool INTERM_PREGENERATED = false;
    // Enable multiple-thread computation
    inline static bool ENABLE_MULTIPLE_THREAD = true;
    // Sum of threads in a process
    inline static int LOCAL_THREADS = static_cast<int>(std::thread::hardware_concurrency() * 100);
    // Used times of a bmt
    inline static int BMT_USAGE_LIMIT = 1;
    // Communication object
    inline static Comm *COMM_IMPL = new MpiComm;
    // Bmt max num
    inline static int MAX_BMTS = 10000;
    // Blocking Bmt Queue
    inline static BlockingQueue<Bmt> *BMT_QUEUE = new CasBlockingQueue<Bmt>(MAX_BMTS);
};



#endif //CONF_H
