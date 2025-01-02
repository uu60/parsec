//
// Created by 杜建璋 on 2024/9/2.
//

#include "compute/arith/ArithLessThanExecutor.h"

#include "comm/IComm.h"
#include "compute/arith/ArithToBoolExecutor.h"
#include "utils/Log.h"

ArithLessThanExecutor::ArithLessThanExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                                             int clientRank) : ArithExecutor(
    x, y, l, taskTag, msgTagOffset, clientRank) {
    _zi = _xi - _yi;
}

ArithLessThanExecutor *ArithLessThanExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        ArithToBoolExecutor e(_zi, _l, _taskTag, _currentMsgTag, -1);
        _zi = e.execute()->_zi;
        _sign = (_zi >> (_l - 1)) & 1;
    }
    return this;
}

ArithLessThanExecutor *ArithLessThanExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_sign, clientRank, buildTag(_currentMsgTag));
    } else {
        bool sign0, sign1;
        IComm::impl->receive(&sign0, 0, buildTag(_currentMsgTag));
        IComm::impl->receive(&sign1, 1, buildTag(_currentMsgTag));
        _result = sign0 ^ sign1;
    }
    return this;
}

std::string ArithLessThanExecutor::className() const {
    return "[ArithLessThanExecutor]";
}

int16_t ArithLessThanExecutor::needsMsgTags(int l) {
    return ArithToBoolExecutor::needsMsgTags(l);
}
