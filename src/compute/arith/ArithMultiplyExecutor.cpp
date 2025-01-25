//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/arith/ArithMultiplyExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/Comm.h"
#include "intermediate/BmtGenerator.h"
#include "utils/Log.h"

ArithMultiplyExecutor *ArithMultiplyExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    // process
    if (Comm::isServer()) {
        Bmt bmt;
        if (_bmt != nullptr) {
            bmt = *_bmt;
        } else if (Conf::INTERM_PREGENERATED) {
            bmt = IntermediateDataSupport::pollBmts(1, _l)[0];
        } else {
            bmt = BmtGenerator(_l, _taskTag, _currentMsgTag).execute()->_bmt;
        }
        int64_t ei = ring(_xi - bmt._a);
        int64_t fi = ring(_yi - bmt._b);
        std::vector efi = {ei, fi};
        std::vector<int64_t> efo;
        Comm::serverSend(efi, buildTag(_currentMsgTag));
        Comm::serverReceive(efo, buildTag(_currentMsgTag));
        int64_t e = ring(ei + efo[0]);
        int64_t f = ring(fi + efo[1]);
        _zi = ring(Comm::rank() * e * f + f * bmt._a + e * bmt._b + bmt._c);
    }

    return this;
}

std::string ArithMultiplyExecutor::className() const {
    return "ArithMultiplyExecutor";
}

int16_t ArithMultiplyExecutor::needMsgTags(int l) {
    return BmtGenerator::needMsgTags(l);
}

ArithMultiplyExecutor *ArithMultiplyExecutor::setBmt(Bmt *bmt) {
    _bmt = bmt;
    return this;
}

int ArithMultiplyExecutor::needBmtBits(int l) {
    return l;
}
