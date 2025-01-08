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
#include "utils/Math.h"

ArithMutexExecutor::ArithMutexExecutor(int64_t x, int64_t y, bool cond, int l, int16_t taskTag, int16_t msgTagOffset,
                                   int clientRank) : ArithExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    if (clientRank < 0 && _l > 1) {
        _cond_i = BoolToArithExecutor(cond, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;
    } else {
        _cond_i = ArithExecutor(cond, _l, _taskTag, _currentMsgTag, clientRank)._zi;
    }
}

ArithMutexExecutor *ArithMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        auto bmts = _bmts == nullptr ? IntermediateDataSupport::pollBmts(2) : *_bmts;
        int64_t cx, cy;
        auto f0 = System::_threadPool.push([this, &bmts](int _) {
            auto mul0 = ArithMultiplyExecutor(_cond_i, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            return mul0.setBmt(&bmts[0])->execute()->_zi;
        });
        auto f1 = System::_threadPool.push([this, &bmts](int _) {
            auto mul1 = ArithMultiplyExecutor(_cond_i, _yi, _l, _taskTag,
                                         static_cast<int16_t>(_currentMsgTag + ArithMultiplyExecutor::needsMsgTags()), NO_CLIENT_COMPUTE);
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

int16_t ArithMutexExecutor::needsMsgTags(int l, int clientRank) {
    if (clientRank < 0 && l > 1) {
        return std::max(static_cast<int16_t>(2 * ArithMultiplyExecutor::needsMsgTags()), BoolToArithExecutor::needsMsgTags(l));
    }
    return static_cast<int16_t>(2 * ArithMultiplyExecutor::needsMsgTags());
}

ArithMutexExecutor *ArithMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
