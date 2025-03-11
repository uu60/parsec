//
// Created by 杜建璋 on 2024/9/2.
//

#include "compute/single/arith/ArithLessExecutor.h"

#include "comm/Comm.h"
#include "compute/single/arith/ArithToBoolExecutor.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

ArithLessExecutor::ArithLessExecutor(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset,
                                     int clientRank) : ArithExecutor(
    x, y, l, taskTag, msgTagOffset, clientRank) {
}

ArithLessExecutor *ArithLessExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        int64_t a_delta = _xi - _yi;
        ArithToBoolExecutor e(a_delta, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
        int64_t b_delta = e.setBmts(_bmts)->execute()->_zi;
        _zi = (b_delta >> (_width - 1)) & 1;
    }
    return this;
}

ArithLessExecutor *ArithLessExecutor::reconstruct(int clientRank) {
    ArithExecutor::reconstruct(clientRank);
    _result &= 1;
    return this;
}

int ArithLessExecutor::msgTagCount(int l) {
    return ArithToBoolExecutor::msgTagCount(l);
}

int ArithLessExecutor::bmtCount(int width) {
    return ArithToBoolExecutor::bmtCount(width);
}

ArithLessExecutor *ArithLessExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Bmt size mismatch.");
    }
    _bmts = bmts;
    return this;
}
