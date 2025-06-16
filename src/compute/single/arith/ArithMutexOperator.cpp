//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/single/arith/ArithMutexOperator.h"

#include "compute/single/arith/ArithMultiplyOperator.h"
#include "comm/Comm.h"
#include "compute/single/bool/BoolOperator.h"
#include "compute/single/bool/BoolToArithOperator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithMutexOperator::ArithMutexOperator(int64_t x, int64_t y, bool cond, int l, int taskTag, int msgTagOffset,
                                       int clientRank) : ArithOperator(x, y, l, taskTag, msgTagOffset, clientRank) {
    if (clientRank < 0 && _width > 1) {
        _cond_i = BoolToArithOperator(cond, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;
    } else {
        _cond_i = ArithOperator(cond, _width, _taskTag, _currentMsgTag, clientRank)._zi;
    }
}

ArithMutexOperator *ArithMutexOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        Bmt bmt0, bmt1;
        if (_bmts != nullptr) {
            bmt0 = _bmts->at(0);
            bmt1 = _bmts->at(1);
        } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
            auto bs = IntermediateDataSupport::pollBitwiseBmts(2, _width);
            bmt0 = bs[0];
            bmt1 = bs[1];
        }
        int64_t cx, cy;
        std::future<int64_t> f;
        auto bp0 = _bmts == nullptr && Conf::BMT_METHOD == Conf::BMT_JIT ? nullptr : &bmt0;
        auto bp1 = _bmts == nullptr && Conf::BMT_METHOD == Conf::BMT_JIT ? nullptr : &bmt1;

        if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
            f = ThreadPoolSupport::submit([&] {
                auto mul0 = ArithMultiplyOperator(_cond_i, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
                int64_t ret = mul0.setBmt(bp0)->execute()->_zi;
                return ret;
            });
        } else {
            cx = ArithMultiplyOperator(_cond_i, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(bp0)
                    ->execute()->_zi;
        }

        auto mul1 = ArithMultiplyOperator(_cond_i, _yi, _width, _taskTag,
                                          static_cast<int>(_currentMsgTag + ArithMultiplyOperator::tagStride(
                                                                   _width)),
                                          NO_CLIENT_COMPUTE);
        cy = mul1.setBmt(bp1)->execute()->_zi;
        if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
            cx = f.get();
        }
        _zi = ring(cx + _yi - cy);
    }
    return this;
}


int ArithMutexOperator::tagStride(int width) {
    return std::max(static_cast<int>(2 * ArithMultiplyOperator::tagStride(width)),
                    BoolToArithOperator::tagStride(width));
}

ArithMutexOperator *ArithMutexOperator::setBmts(std::vector<Bmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Mismatch bmt size.");
    }
    _bmts = bmts;
    return this;
}

int ArithMutexOperator::bmtCount(int width) {
    return 2;
}
