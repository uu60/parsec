//
// Created by 杜建璋 on 2024/9/2.
//

#include "compute/arith/ArithLessThanExecutor.h"

#include <utility>

#include "comm/IComm.h"
#include "compute/arith/ArithToBoolExecutor.h"

ArithLessThanExecutor::ArithLessThanExecutor(int64_t x, int64_t y, int l, int32_t objTag, int8_t msgTagOffset,
                                             int clientRank) : ArithExecutor(
    x, y, l, objTag, msgTagOffset, clientRank) {
    _zi = ring(_xi - _yi);
}

ArithLessThanExecutor *ArithLessThanExecutor::execute() {
    if (IComm::impl->isServer()) {
        ArithToBoolExecutor e(_zi, _l, _objTag, _currentMsgTag, -1);
        _currentMsgTag = static_cast<int8_t>(_currentMsgTag + ArithToBoolExecutor::msgNum());
        _zi = e.execute()->_zi;
        _sign = (_zi >> (_l - 1)) & 1;
    }
    return this;
}

ArithLessThanExecutor *ArithLessThanExecutor::reconstruct(int clientRank) {
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

int8_t ArithLessThanExecutor::msgNum() {
    return ArithToBoolExecutor::msgNum();
}
