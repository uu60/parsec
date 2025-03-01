//
// Created by 杜建璋 on 2024/7/26.
//

#include "utils/System.h"

#include "comm/Comm.h"
#include "comm/MpiComm.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

void System::init(int argc, char **argv) {
    // prepare structures
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

int16_t System::nextTask() {
    if (_currentTaskTag < 0 || _currentTaskTag < PRESERVED_TASK_TAGS) {
        _currentTaskTag = PRESERVED_TASK_TAGS;
    }
    int16_t ret = _currentTaskTag;
    _currentTaskTag = static_cast<int16_t>(Math::ring(_currentTaskTag + 1, 16));
    return ret;
}

int64_t System::currentTimeMillis() {
    auto now = std::chrono::system_clock::now();

    auto duration = now.time_since_epoch();
    long millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    return millis;
}
