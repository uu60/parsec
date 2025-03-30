//
// Created by 杜建璋 on 2025/3/26.
//

#include <vector>

#include "../include/compute/batch/bool/BoolAndBatchExecutor.h"
#include "../include/compute/batch/bool/BoolLessBatchExecutor.h"
#include "../include/compute/batch/bool/BoolMutexBatchExecutor.h"
#include "../include/compute/single/bool/BoolAndExecutor.h"
#include "../include/compute/single/bool/BoolLessExecutor.h"
#include "../include/compute/single/bool/BoolMutexExecutor.h"
#include "../include/intermediate/BitwiseBmtGenerator.h"
#include "../include/ot/RandOtBatchExecutor.h"
#include "../include/secret/Secrets.h"
#include "../include/secret/item/BoolSecret.h"
#include "../include/utils/Log.h"
#include "../include/utils/System.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int num = 10000;
    int width = 64;

    for (int i = 0; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-num" && i + 1 < argc) {
            num = std::stoi(argv[i + 1]);
            continue;
        }
        if (arg == "-width" && i + 1 < argc) {
            width = std::stoi(argv[i + 1]);
        }
    }

    std::vector<BoolSecret> arr;

    auto t = System::nextTask();
    for (int i = 0; i < num; i++) {
        arr.push_back(BoolSecret(num - i, width, t, 0).share(2));
    }

    if (Comm::isServer()) {
        auto start = System::currentTimeMillis();

        Secrets::sort(arr, true, t);

        Log::i("total time: {}ms", System::currentTimeMillis() - start);
        if (Conf::ENABLE_CLASS_WISE_TIMING) {
            Log::i("less than: {}ms", BoolLessExecutor::_totalTime + BoolLessBatchExecutor::_totalTime);
            Log::i("bool and: {}ms", BoolAndExecutor::_totalTime + BoolAndBatchExecutor::_totalTime);
            Log::i("comm: {}ms", Comm::_totalTime);
            Log::i("mux time: {}ms", BoolMutexExecutor::_totalTime + BoolMutexBatchExecutor::_totalTime);
            Log::i("bmt gen: {}ms", BitwiseBmtGenerator::_totalTime);
            Log::i("ot: {}ms", RandOtBatchExecutor::_totalTime);
        }
    }

    std::vector<BoolSecret> res;
    for (int i = 0; i < num; i++) {
        res.push_back(arr[i].task(3).reconstruct(2));
    }

    if (Comm::isClient()) {
        int last = INT_MIN;
        for (auto s: res) {
            if (s._data <= last) {
                Log::i("Wrong: {}", s._data);
            }
            last = s._data;
        }
    }
    System::finalize();
}
