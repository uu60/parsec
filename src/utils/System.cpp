//
// Created by 杜建璋 on 2024/7/26.
//

#include "utils/System.h"

#include "base/AbstractSecureExecutor.h"
#include "comm/Comm.h"
#include "comm/MpiComm.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

void System::init(int argc, char **argv) {
    Conf::init(argc, argv);

    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_BATCH_BACKGROUND) {
        PRESERVED_TASK_TAGS = 2;
    }

    // prepare structures
    // Thread pool needs implementing at first because other initialization may use it.
    ThreadPoolSupport::init();

    // init comm
    Comm::init(argc, argv);

    // start produce
    IntermediateDataSupport::init();
    Log::i("System initialized.");
}

void System::finalize() {
    Log::i("Prepare to shutdown...");
    // Wait all threads done
    _shutdown = true;
    // Wait for generators to stop
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // finalize comm
    Comm::finalize();
    Log::i("System shut down.");
}

// nextTask should not be accessed in parallel in case the sequence is wrong
int System::nextTask() {
    int nextTask = _currentTaskTag & Conf::TASK_TAG_BITS;

    if (nextTask < 0 || nextTask < PRESERVED_TASK_TAGS) {
        _currentTaskTag = PRESERVED_TASK_TAGS;
    }

    return _currentTaskTag;
}

int64_t System::currentTimeMillis() {
    auto now = std::chrono::system_clock::now();

    auto duration = now.time_since_epoch();
    long millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    return millis;
}
