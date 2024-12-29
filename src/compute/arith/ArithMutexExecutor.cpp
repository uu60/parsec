//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/arith/ArithMutexExecutor.h"

#include "compute/arith/ArithMultiplyExecutor.h"
#include "comm/IComm.h"
#include "compute/bool/BoolExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

ArithMutexExecutor::ArithMutexExecutor(int64_t x, int64_t y, bool c, int l, int16_t objTag, int16_t msgTagOffset,
                                   int clientRank) : ArithExecutor(x, y, l, objTag, msgTagOffset, clientRank) {
    if (clientRank < 0 && _l > 1) {
        _cond_i = BoolToArithExecutor(c, 1, _objTag, _currentMsgTag, -1).execute()->_zi;
    } else {
        _cond_i = ArithExecutor(c, 1, _objTag, _currentMsgTag, clientRank)._zi;
    }
}

ArithMutexExecutor *ArithMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        auto bmts = _bmts == nullptr ? IntermediateDataSupport::pollBmts(2) : *_bmts;
        int64_t cx, cy;
        auto f0 = System::_threadPool.push([this, &bmts](int _) {
            auto mul0 = ArithMultiplyExecutor(_cond_i, _xi, _l, _objTag,
                                         _currentMsgTag, -1);
            return mul0.setBmt(&bmts[0])->execute()->_zi;
        });
        auto f1 = System::_threadPool.push([this, &bmts](int _) {
            auto mul1 = ArithMultiplyExecutor(_cond_i, _yi, _l, _objTag,
                                         static_cast<int16_t>(_currentMsgTag + ArithMultiplyExecutor::neededMsgTags()), -1);
            return mul1.setBmt(&bmts[1])->execute()->_zi;
        });
        cx = f0.get();
        cy = f1.get();
        _zi = ring(cx + _yi - cy);
    }
    return this;
}

std::string ArithMutexExecutor::className() const {
    return "[MuxArithExecutor]";
}

int64_t ArithMutexExecutor::neededMsgTags() {
    return static_cast<int16_t>(2 * ArithMultiplyExecutor::neededMsgTags());
}

ArithMutexExecutor *ArithMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
