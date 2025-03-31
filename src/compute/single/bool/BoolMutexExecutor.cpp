//
// Created by 杜建璋 on 2025/1/8.
//

#include "compute/single/bool/BoolMutexExecutor.h"

#include "compute/batch/bool/BoolAndBatchExecutor.h"
#include "compute/single/bool/BoolAndExecutor.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

BoolMutexExecutor::BoolMutexExecutor(int64_t x, int64_t y, bool cond, int width, int taskTag, int msgTagOffset,
                                     int clientRank) : BoolExecutor(x, y, width, taskTag, msgTagOffset, clientRank) {
    _cond_i = BoolExecutor(cond, 1, _taskTag, _currentMsgTag, clientRank)._zi;
    if (_cond_i) {
        // Set to all 1 on each bit
        _cond_i = ring(-1ll);
    }
}

bool BoolMutexExecutor::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}

BoolMutexExecutor *BoolMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    int64_t cx, cy;

    std::vector conds = {_cond_i, _cond_i};
    std::vector xy = {_xi, _yi};
    auto temp = BoolAndBatchExecutor(&conds, &xy, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;
    cx = temp[0];
    cy = temp[1];

    _zi = ring(cx ^ _yi ^ cy);

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolMutexExecutor *BoolMutexExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount()) {
        throw std::runtime_error("Mismatched bmts count");
    }
    _bmts = bmts;
    return this;
}

int BoolMutexExecutor::msgTagCount(int width) {
    return 2 * BoolAndExecutor::msgTagCount(width);
}

int BoolMutexExecutor::bmtCount() {
    return 2;
}
