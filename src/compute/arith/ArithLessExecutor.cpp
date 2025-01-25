//
// Created by 杜建璋 on 2024/9/2.
//

#include "compute/arith/ArithLessExecutor.h"

#include "comm/Comm.h"
#include "compute/arith/ArithToBoolExecutor.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

ArithLessExecutor::ArithLessExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                                     int clientRank) : ArithExecutor(
    x, y, l, taskTag, msgTagOffset, clientRank) {
}

ArithLessExecutor *ArithLessExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        int64_t a_delta = _xi - _yi;
        ArithToBoolExecutor e(a_delta, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);

        // if (_bmts != nullptr) {
        //     b_delta = e.setBmts(_bmts)->execute()->_zi;
        // } else {
        //     std::vector<Bmt> bmts;
        //     // first is num of bmt, second is bit len of bmt
        //     auto bmtInfo = ArithToBoolExecutor::needBmtsWithBits(_l);
        //     if (Conf::INTERM_PREGENERATED) {
        //         bmts = IntermediateDataSupport::pollBmts(bmtInfo.first, bmtInfo.second);
        //     } else {
        //         bmts.reserve(bmtInfo.first);
        //         for (int i = 0; i < bmtInfo.first; i++) {
        //             bmts[i] = BmtGenerator(_l, _taskTag, _currentMsgTag).execute()->_bmt;
        //         }
        //     }
        //     b_delta = e.setBmts(&bmts)->execute()->_zi;
        // }
        int64_t b_delta = e.setBmts(_bmts)->execute()->_zi;
        _zi = (b_delta >> (_l - 1)) & 1;
    }
    return this;
}

ArithLessExecutor *ArithLessExecutor::reconstruct(int clientRank) {
    ArithExecutor::reconstruct(clientRank);
    _result &= 1;
    return this;
}

std::string ArithLessExecutor::className() const {
    return "[ArithLessThanExecutor]";
}

int16_t ArithLessExecutor::needMsgTags() {
    return ArithToBoolExecutor::needMsgTags();
}

std::pair<int, int> ArithLessExecutor::needBmtsWithBits(int l) {
    return ArithToBoolExecutor::needBmtsWithBits(l);
}

ArithLessExecutor *ArithLessExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
