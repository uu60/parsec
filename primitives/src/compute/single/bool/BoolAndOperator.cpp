
#include "compute/single/bool/BoolAndOperator.h"

#include "compute/single/arith/ArithMultiplyOperator.h"
#include "comm/Comm.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolAndOperator *BoolAndOperator::execute() {
    _currentMsgTag = _startMsgTag;

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_bmt == nullptr) {
        if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
            _bmt = &IntermediateDataSupport::_fixedBitwiseBmt;
        } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
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

    auto r = Comm::serverSendAsync(efi, _width, buildTag(_currentMsgTag));
    Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
    Comm::wait(r);

    e = ring(ei ^ efo[0]);
    f = ring(fi ^ efo[1]);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    _zi = (extendedRank & e & f) ^ (f & _bmt->_a) ^ (e & _bmt->_b) ^ _bmt->_c;

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int BoolAndOperator::tagStride(int width) {
    return BitwiseBmtGenerator::tagStride(width);
}

BoolAndOperator *BoolAndOperator::setBmt(BitwiseBmt *bmt) {
    _bmt = bmt;
    return this;
}
