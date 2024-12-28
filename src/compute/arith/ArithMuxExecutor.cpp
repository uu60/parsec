//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/arith/ArithMuxExecutor.h"

#include "compute/arith/ArithMulExecutor.h"
#include "comm/IComm.h"
#include "compute/BoolExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

ArithMuxExecutor::ArithMuxExecutor(int64_t x, int64_t y, bool c, int l, int16_t objTag, int16_t msgTagOffset,
                                   int clientRank) : ArithExecutor(x, y, l, objTag, msgTagOffset, clientRank) {
    if (clientRank < 0 && _l > 1) {
        _cond_i = BoolToArithExecutor(c, 1, _objTag, _currentMsgTag, -1).execute()->_zi;
    } else {
        _cond_i = ArithExecutor(c, 1, _objTag, _currentMsgTag, clientRank)._zi;
    }
}

ArithMuxExecutor *ArithMuxExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        auto bmts = _bmts == nullptr ? IntermediateDataSupport::pollBmts(2) : *_bmts;
        int64_t cx, cy;
        auto f0 = System::_threadPool.push([this, &bmts](int _) {
            auto mul0 = ArithMulExecutor(_cond_i, _xi, _l, _objTag,
                                         _currentMsgTag, -1);
            return mul0.setBmt(&bmts[0])->execute()->_zi;
        });
        auto f1 = System::_threadPool.push([this, &bmts](int _) {
            auto mul1 = ArithMulExecutor(_cond_i, _yi, _l, _objTag,
                                         static_cast<int16_t>(_currentMsgTag + ArithMulExecutor::neededMsgTags()), -1);
            return mul1.setBmt(&bmts[1])->execute()->_zi;
        });
        cx = f0.get();
        cy = f1.get();
        _zi = ring(cx + _yi - cy);
    }
    return this;
}

std::string ArithMuxExecutor::className() const {
    return "[MuxArithExecutor]";
}

int64_t ArithMuxExecutor::neededMsgTags() {
    return static_cast<int16_t>(2 * ArithMulExecutor::neededMsgTags());
}

ArithMuxExecutor *ArithMuxExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
