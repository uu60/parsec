
#include "parsecdb/utils/System.h"

#include "parsecdb/base/SecureOperator.h"
#include "parsecdb/comm/Comm.h"
#include "parsecdb/comm/MpiComm.h"
#include "parsecdb/conf/Conf.h"
#include "parsecdb/intermediate/IntermediateDataSupport.h"
#include "parsecdb/parallel/ThreadPoolSupport.h"
#include "parsecdb/utils/Log.h"
#include "parsecdb/utils/Math.h"

namespace parsecdb {

void System::init(int argc, char** argv) {
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

} // namespace parsecdb
