//
// Created by 杜建璋 on 2025/3/26.
//

#include <vector>
#include <climits>

#include "../include/compute/batch/bool/BoolAndBatchOperator.h"
#include "../include/compute/batch/bool/BoolLessBatchOperator.h"
#include "../include/compute/batch/bool/BoolMutexBatchOperator.h"
#include "../include/compute/single/bool/BoolAndOperator.h"
#include "../include/compute/single/bool/BoolLessOperator.h"
#include "../include/compute/single/bool/BoolMutexOperator.h"
#include "../include/intermediate/BitwiseBmtGenerator.h"
#include "../include/ot/RandOtBatchOperator.h"
#include "../include/secret/Secrets.h"
#include "../include/secret/item/BoolSecret.h"
#include "../include/utils/Log.h"
#include "../include/utils/System.h"
#include "utils/StringUtils.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    int num = 10000;
    int width = 64;

    if (Conf::_userParams.count("num")) {
        num = std::stoi(Conf::_userParams["num"]);
    }

    if (Conf::_userParams.count("width")) {
        width = std::stoi(Conf::_userParams["width"]);
    }

    std::vector<BoolSecret> arr;

    auto t = System::nextTask();
    for (int i = 0; i < num; i++) {
        arr.push_back(BoolSecret(num - i, width, t, 0).share(2));
    }

    if (Comm::isServer()) {
        auto start = System::currentTimeMillis();

        Secrets::sort(arr, false, t);

        Log::i("total time: {}ms", System::currentTimeMillis() - start);
        if (Conf::ENABLE_CLASS_WISE_TIMING) {
            Log::i("less than: {}ms", BoolLessOperator::_totalTime + BoolLessBatchOperator::_totalTime);
            Log::i("bool and: {}ms", BoolAndOperator::_totalTime + BoolAndBatchOperator::_totalTime);
            Log::i("comm: {}ms", Comm::_totalTime);
            Log::i("mux time: {}ms", BoolMutexOperator::_totalTime + BoolMutexBatchOperator::_totalTime);
            Log::i("bmt gen: {}ms", BitwiseBmtGenerator::_totalTime);
            Log::i("ot: {}ms", RandOtBatchOperator::_totalTime);
        }
    }

    std::vector<BoolSecret> res;
    std::vector<int64_t> intRes;
    for (int i = 0; i < num; i++) {
        res.push_back(arr[i].task(3).reconstruct(2));
        intRes.push_back(res[i]._data);
    }

    if (Comm::isClient()) {
        int last = INT_MIN;
        Log::i("Result: {}", StringUtils::vecString(intRes));
        for (auto s: res) {
            if (s._data < last) {
                Log::i("Wrong: {}", s._data);
            }
            last = s._data;
        }
    }
    System::finalize();
}
