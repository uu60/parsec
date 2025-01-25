//
// Created by 杜建璋 on 2024/7/26.
//

#include "utils/System.h"

#include "comm/Comm.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

std::atomic_bool System::_shutdown = false;
ctpl::thread_pool System::_threadPool(Conf::LOCAL_THREADS);

void System::init(Comm *impl, int argc, char **argv) {
    // init comm
    Comm::impl = impl;
    Comm::init(argc, argv);

    // start produce
    if (Conf::INTERM_PREGENERATED) {
        IntermediateDataSupport::startGenerateBmtsAsync();
    }
    // IntermediateDataSupport::startGenerateABPairsAsyc();
}

void System::finalize() {
    Log::i("Prepare to shutdown...");
    // Wait all threads done
    _shutdown = true;
    // Wait for generators to stop
    std::this_thread::sleep_for(std::chrono::seconds(1));
    _threadPool.stop(false);
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
