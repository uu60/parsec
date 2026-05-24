
#include "utils/System.h"

#include "base/SecureOperator.h"
#include "comm/Comm.h"
#include "comm/MpiComm.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

void System::init(int argc, char **argv) {
    Conf::init(argc, argv);

    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        PRESERVED_TASK_TAGS = Conf::BMT_QUEUE_NUM;
        if (!Conf::DISABLE_ARITH) {
            PRESERVED_TASK_TAGS *= 2;
        }
    }

    ThreadPoolSupport::init();

    Comm::init(argc, argv);

    IntermediateDataSupport::init();
    Log::i("System initialized.");
}

void System::finalize() {
    Log::i("Prepare to shutdown... (if not finalized please press Ctrl + C)");
    _shutdown = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    Comm::finalize();
    ThreadPoolSupport::finalize();
    IntermediateDataSupport::finalize();
    if (Comm::rank() == 0) {
        std::cout << "System shut down." << std::endl;
    }
}

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
