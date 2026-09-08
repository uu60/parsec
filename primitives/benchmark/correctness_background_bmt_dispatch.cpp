#include "comm/Comm.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "secret/Secrets.h"
#include "utils/Log.h"
#include "utils/System.h"

#include <cstdint>
#include <vector>

int main(int argc, char **argv) {
    System::init(argc, argv);

    const int task = System::nextTask();
    constexpr int width = 64;
    std::vector<int64_t> xs, ys;
    if (Comm::isClient()) {
        xs = {0, 1, 2, 7, 9, 15, 42, 99};
        ys = {1, 1, 9, 3, 9, 16, 41, 100};
    }

    auto xShares = Secrets::boolShare(xs, 2, width, task);
    auto yShares = Secrets::boolShare(ys, 2, width, task);

    std::vector<int64_t> resultShares;
    if (Comm::isServer()) {
        auto bmts = IntermediateDataSupport::pollBitwiseBmts(
            BoolLessBatchOperator::bmtCount(static_cast<int>(xShares.size()), width), 64);
        resultShares = BoolLessBatchOperator(&xShares, &yShares, width, task, 0,
                                             SecureOperator::NO_CLIENT_COMPUTE)
                           .setBmts(&bmts)->execute()->_zis;
    }

    auto result = Secrets::boolReconstruct(resultShares, 2, 1, task);
    if (Comm::isClient()) {
        const std::vector<int64_t> expected = {1, 0, 1, 0, 0, 1, 0, 1};
        if (result == expected) {
            Log::i("[Background BMT dispatch correctness] PASS");
        } else {
            Log::e("[Background BMT dispatch correctness] FAIL");
            System::finalize();
            return 1;
        }
    }

    System::finalize();
    return 0;
}
