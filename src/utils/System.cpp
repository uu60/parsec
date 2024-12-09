//
// Created by 杜建璋 on 2024/7/26.
//

#include "utils/System.h"

#include "comm/IComm.h"
#include "intermediate/IntermediateDataSupport.h"

std::atomic_bool System::_shutdown = false;
ctpl::thread_pool System::_threadPool(std::thread::hardware_concurrency() * 8);

void System::init(IComm *impl, int argc, char **argv) {
    // init comm
    IComm::impl = impl;
    IComm::impl->init(argc, argv);

    // start produce
    IntermediateDataSupport::startGenerateBmtsAsync();
    IntermediateDataSupport::startGenerateABPairsAsyc();
}

void System::finalize() {
    // Wait all threads done
    _shutdown = true;
    _threadPool.stop(true);
    // finalize comm
    IComm::impl->finalize();
    std::cout << "System shut down." << std::endl;
}


int64_t System::currentTimeMillis()  {
    auto now = std::chrono::system_clock::now();

    auto duration = now.time_since_epoch();
    long millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    return millis;
}
