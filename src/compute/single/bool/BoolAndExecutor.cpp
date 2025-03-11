//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/single/bool/BoolAndExecutor.h"

#include "compute/single/arith/ArithMultiplyExecutor.h"
#include "comm/Comm.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolAndExecutor *BoolAndExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    int64_t start;
    if (Conf::CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_bmt == nullptr) {
        if (Conf::BMT_METHOD == Consts::BMT_FIXED) {
            _bmt = &IntermediateDataSupport::_fixedBitwiseBmt;
        } else if (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
            auto bmt = IntermediateDataSupport::pollBitwiseBmts(1, _width)[0];
            _bmt = &bmt;
        } else {
            auto bmt = BitwiseBmtGenerator(_width, _taskTag, _currentMsgTag).execute()->_bmt;
            _bmt = &bmt;
        }
    }

    int64_t ei = _xi ^ _bmt->_a;
    int64_t fi = _yi ^ _bmt->_b;


    int64_t e, f;
    std::vector<int64_t> efo;
    std::vector efi = {ei, fi};
    Comm::serverSend(efi, _width, buildTag(_currentMsgTag));
    Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
    e = ring(ei ^ efo[0]);
    f = ring(fi ^ efo[1]);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    _zi = (extendedRank & e & f) ^ (f & _bmt->_a) ^ (e & _bmt->_b) ^ _bmt->_c;

    if (Conf::CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int BoolAndExecutor::msgTagCount(int width) {
    return BitwiseBmtGenerator::msgTagCount(width);
}

BoolAndExecutor *BoolAndExecutor::setBmt(BitwiseBmt *bmt) {
    _bmt = bmt;
    return this;
}
