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

BoolMutexExecutor *BoolMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if constexpr (Conf::CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    BitwiseBmt bmt0, bmt1;
    bool gotBmt = false;
    if (_bmts != nullptr) {
        gotBmt = true;
        bmt0 = _bmts->at(0);
        bmt1 = _bmts->at(1);
    } else if constexpr (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
        gotBmt = true;
        auto bs = IntermediateDataSupport::pollBitwiseBmts(2, _width);
        bmt0 = bs[0];
        bmt1 = bs[1];
    }

    int64_t cx, cy;
    std::future<int64_t> f;
    auto bp0 = gotBmt ? &bmt0 : nullptr;
    auto bp1 = gotBmt ? &bmt1 : nullptr;

    if constexpr (Conf::BMT_METHOD == Consts::BMT_BATCH_BACKGROUND) {
        std::vector conds = {_cond_i, _cond_i};
        std::vector xy = {_xi, _yi};
        auto temp = BoolAndBatchExecutor(conds, xy, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zis;
        cx = temp[0];
        cy = temp[1];
    } else {
        if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
            f = ThreadPoolSupport::submit([&] {
                return BoolAndExecutor(_cond_i, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(bp0)->
                        execute()->_zi;
            });
        } else {
            cx = BoolAndExecutor(_cond_i, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(bp0)->execute()->
                    _zi;
        }

        cy = BoolAndExecutor(_cond_i, _yi, _width, _taskTag,
                             static_cast<int>(_currentMsgTag + BoolAndExecutor::msgTagCount(_width)),
                             NO_CLIENT_COMPUTE).setBmt(bp1)->execute()->_zi;
        if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
            cx = f.get();
        }
    }

    _zi = ring(cx ^ _yi ^ cy);

    if constexpr (Conf::CLASS_WISE_TIMING) {
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
