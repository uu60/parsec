#include "compute/batch/arith/ArithEqualBatchOperator.h"

#include "comm/Comm.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "conf/Conf.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

ArithEqualBatchOperator::ArithEqualBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width,
                                                 int taskTag, int msgTagOffset, int clientRank)
    : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
}

ArithEqualBatchOperator *ArithEqualBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    if (Conf::BMT_METHOD != Conf::BMT_JIT) {
        throw std::runtime_error("Temporarily only support BMT JIT generation for experiment.");
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    // std::vector<int64_t> xy = *_xis;
    // xy.insert(xy.end(), _yis->begin(), _yis->end());
    // std::vector<int64_t> yx = *_yis;
    // yx.insert(yx.end(), _xis->begin(), _xis->end());
    //
    // ArithLessBatchOperator ltOp(&xy, &yx, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    // auto gtv_ltv = ltOp.execute()->_zis;
    //
    // for (auto &v: gtv_ltv) {
    //     v = v ^ Comm::rank();
    // }
    //
    // std::vector<int64_t> ltv(gtv_ltv.begin() + gtv_ltv.size() / 2, gtv_ltv.end());
    // std::vector<int64_t> gtv = std::move(gtv_ltv);
    // gtv.resize(gtv.size() / 2);
    std::vector<int64_t> ltv = ArithLessBatchOperator(_xis, _yis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            execute()->_zis;
    std::vector<int64_t> gtv = ArithLessBatchOperator(_yis, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            execute()->_zis;
    for (auto &v: ltv) {
        v = v ^ Comm::rank();
    }
    for (auto &v: gtv) {
        v = v ^ Comm::rank();
    }

    _zis = BoolAndBatchOperator(&gtv, &ltv, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zis;


    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

ArithEqualBatchOperator *ArithEqualBatchOperator::reconstruct(int clientRank) {
    ArithBatchOperator::reconstruct(clientRank);

    if (Comm::rank() == clientRank) {
        for (int i = 0; i < _results.size(); i++) {
            _results[i] &= 1;
        }
    }

    return this;
}

int ArithEqualBatchOperator::tagStride(int width) {
    return std::max(ArithLessBatchOperator::tagStride(width), BoolAndBatchOperator::tagStride());
}
