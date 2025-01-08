//
// Created by 杜建璋 on 2024/9/2.
//

#include "compute/arith/ArithLessExecutor.h"

#include "comm/IComm.h"
#include "compute/arith/ArithToBoolExecutor.h"
#include "utils/Log.h"

ArithLessExecutor::ArithLessExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                                             int clientRank) : ArithExecutor(
    x, y, l, taskTag, msgTagOffset, clientRank) {}

ArithLessExecutor *ArithLessExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    int64_t a_delta = _xi - _yi;
    if (IComm::impl->isServer()) {
        ArithToBoolExecutor e(a_delta, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
        int64_t b_delta = e.execute()->_zi;
        _zi = (b_delta >> (_l - 1)) & 1;
    }
    return this;
}

ArithLessExecutor *ArithLessExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_zi, clientRank, buildTag(_currentMsgTag));
    } else {
        int64_t sign0, sign1;
        IComm::impl->receive(&sign0, 0, buildTag(_currentMsgTag));
        IComm::impl->receive(&sign1, 1, buildTag(_currentMsgTag));
        _result = sign0 ^ sign1;
    }
    return this;
}

std::string ArithLessExecutor::className() const {
    return "[ArithLessThanExecutor]";
}

int16_t ArithLessExecutor::needsMsgTags(int l) {
    return ArithToBoolExecutor::needsMsgTags(l);
}
