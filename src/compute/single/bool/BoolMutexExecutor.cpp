//
// Created by 杜建璋 on 2025/1/8.
//

#include "compute/single/bool/BoolMutexExecutor.h"

#include "compute/single/bool/BoolAndExecutor.h"
#include "intermediate/IntermediateDataSupport.h"

BoolMutexExecutor::BoolMutexExecutor(int64_t x, int64_t y, bool cond, int width, int16_t taskTag, int16_t msgTagOffset,
                                     int clientRank) : BoolExecutor(x, y, width, taskTag, msgTagOffset, clientRank) {
    _cond_i = BoolExecutor(cond, 1, _taskTag, _currentMsgTag, clientRank)._zi;
    if (_cond_i) {
        // Set to all 1 on each bit
        _cond_i = ring(-1ll);
    }
}

BoolMutexExecutor *BoolMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        BitwiseBmt bmt0, bmt1;
        if (_bmts != nullptr) {
            bmt0 = _bmts->at(0);
            bmt1 = _bmts->at(1);
        } else if (Conf::BMT_BACKGROUND) {
            auto bs = IntermediateDataSupport::pollBitwiseBmts(2, _width);
            bmt0 = bs[0];
            bmt1 = bs[1];
        }

        int64_t cx, cy;
        std::future<int64_t> f;
        auto bp0 = _bmts == nullptr && !Conf::BMT_BACKGROUND ? nullptr : &bmt0;
        auto bp1 = _bmts == nullptr && !Conf::BMT_BACKGROUND ? nullptr : &bmt1;

        if (Conf::INTRA_OPERATOR_PARALLELISM) {
            f = System::_threadPool.push([&](int) {
                return BoolAndExecutor(_cond_i, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(bp0)->execute()->_zi;
            });
        } else {
            cx = BoolAndExecutor(_cond_i, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(bp0)->execute()->_zi;
        }

        cy = BoolAndExecutor(_cond_i, _yi, _width, _taskTag,
                             static_cast<int16_t>(_currentMsgTag + BoolAndExecutor::msgTagCount(_width)),
                             NO_CLIENT_COMPUTE).setBmt(bp1)->execute()->_zi;
        if (Conf::INTRA_OPERATOR_PARALLELISM) {
            cx = f.get();
        }

        _zi = ring(cx ^ _yi ^ cy);
    }
    return this;
}

BoolMutexExecutor *BoolMutexExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Mismatched bmts count");
    }
    _bmts = bmts;
    return this;
}

int16_t BoolMutexExecutor::msgTagCount(int l) {
    return static_cast<int16_t>(2 * BoolAndExecutor::msgTagCount(l));
}

int BoolMutexExecutor::bmtCount(int width) {
    return 2;
}
