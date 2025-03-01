//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/single/arith/ArithMultiplyExecutor.h"

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
        } else if (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
            bmt = IntermediateDataSupport::pollBmts(1, _width)[0];
        } else {
            bmt = BmtGenerator(_width, _taskTag, _currentMsgTag).execute()->_bmt;
        }
        int64_t ei = ring(_xi - bmt._a);
        int64_t fi = ring(_yi - bmt._b);
        std::vector efi = {ei, fi};
        std::vector<int64_t> efo;
        Comm::serverSend(efi, _width, buildTag(_currentMsgTag));
        Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
        int64_t e = ring(ei + efo[0]);
        int64_t f = ring(fi + efo[1]);
        _zi = ring(Comm::rank() * e * f + f * bmt._a + e * bmt._b + bmt._c);
    }

    return this;
}

int16_t ArithMultiplyExecutor::msgTagCount(int width) {
    return BmtGenerator::msgTagCount(width);
}

ArithMultiplyExecutor *ArithMultiplyExecutor::setBmt(Bmt *bmt) {
    _bmt = bmt;
    return this;
}
