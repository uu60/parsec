//
// Created by 杜建璋 on 2024/7/26.
//

#include "utils/System.h"

#include "comm/IComm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

std::atomic_bool System::_shutdown = false;
ctpl::thread_pool System::_threadPool(static_cast<int>(std::thread::hardware_concurrency() * 20));

void System::init(IComm *impl, int argc, char **argv) {
    // init comm
    IComm::impl = impl;
    IComm::impl->init(argc, argv);

    // start produce
    IntermediateDataSupport::startGenerateBmtsAsync();
    IntermediateDataSupport::startGenerateABPairsAsyc();
}

void System::finalize() {
    Log::i("Prepare to shutdown...");
    // Wait all threads done
    _shutdown = true;
    // Wait for generators to stop
    std::this_thread::sleep_for(std::chrono::seconds(1));
    _threadPool.stop(false);
    // finalize comm
    IComm::impl->finalize();
    Log::i("System shut down.");
}

int16_t System::nextTask() {
    if (_currentTaskTag < 0 || _currentTaskTag < _preservedTaskTags) {
        _currentTaskTag = _preservedTaskTags;
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
