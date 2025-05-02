//
// Created by 杜建璋 on 2025/1/8.
//

#include "compute/single/bool/BoolMutexOperator.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/single/bool/BoolAndOperator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

BoolMutexOperator::BoolMutexOperator(int64_t x, int64_t y, bool cond, int width, int taskTag, int msgTagOffset,
                                     int clientRank) : BoolOperator(x, y, width, taskTag, msgTagOffset, clientRank) {
    _cond_i = BoolOperator(cond, 1, _taskTag, _currentMsgTag, clientRank)._zi;
    if (_cond_i) {
        // Set to all 1 on each bit
        _cond_i = ring(-1ll);
    }
}

bool BoolMutexOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}

BoolMutexOperator *BoolMutexOperator::execute() {
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
    auto temp = BoolAndBatchOperator(&conds, &xy, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;
    cx = temp[0];
    cy = temp[1];

    _zi = ring(cx ^ _yi ^ cy);

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolMutexOperator *BoolMutexOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount()) {
        throw std::runtime_error("Mismatched bmts count");
    }
    _bmts = bmts;
    return this;
}

int BoolMutexOperator::msgTagCount(int width) {
    return 2 * BoolAndOperator::msgTagCount(width);
}

int BoolMutexOperator::bmtCount() {
    return 2;
}
