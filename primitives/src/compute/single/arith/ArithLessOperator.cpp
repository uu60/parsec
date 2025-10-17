
#include "compute/single/arith/ArithLessOperator.h"

#include "comm/Comm.h"
#include "compute/single/arith/ArithToBoolOperator.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

ArithLessOperator::ArithLessOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset,
                                     int clientRank) : ArithOperator(
    x, y, l, taskTag, msgTagOffset, clientRank) {
}

ArithLessOperator *ArithLessOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t a_delta = _xi - _yi;
    ArithToBoolOperator e(a_delta, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    int64_t b_delta = e.setBmts(_bmts)->execute()->_zi;
    _zi = (b_delta >> (_width - 1)) & 1;

    return this;
}

ArithLessOperator *ArithLessOperator::reconstruct(int clientRank) {
    ArithOperator::reconstruct(clientRank);
    _result &= 1;
    return this;
}

int ArithLessOperator::tagStride(int l) {
    return ArithToBoolOperator::tagStride(l);
}

int ArithLessOperator::bmtCount(int width) {
    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        return 0;
    }
    return ArithToBoolOperator::bmtCount(width);
}

ArithLessOperator *ArithLessOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Bmt size mismatch.");
    }
    _bmts = bmts;
    return this;
}
