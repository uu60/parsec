
#include "parsecdb/compute/single/bool/BoolAndOperator.h"

#include "parsecdb/comm/Comm.h"
#include "parsecdb/compute/single/arith/ArithMultiplyOperator.h"
#include "parsecdb/intermediate/BitwiseBmtGenerator.h"
#include "parsecdb/intermediate/IntermediateDataSupport.h"
#include "parsecdb/utils/Log.h"
#include "parsecdb/utils/Math.h"

namespace parsecdb {

BoolAndOperator* BoolAndOperator::execute() {
    _currentMsgTag = _startMsgTag;

    int64_t start = 0;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    BitwiseBmt generatedBmt;
    BitwiseBmt* bmt = _bmt;
    if (bmt == nullptr) {
        if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
            bmt = &IntermediateDataSupport::_fixedBitwiseBmt;
        } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
            generatedBmt = IntermediateDataSupport::pollBitwiseBmts(1, _width)[0];
            bmt = &generatedBmt;
        } else {
            generatedBmt = BitwiseBmtGenerator(_width, _taskTag, _currentMsgTag).execute()->_bmt;
            bmt = &generatedBmt;
        }
    }

    int64_t ei = _xi ^ bmt->_a;
    int64_t fi = _yi ^ bmt->_b;

    int64_t e, f;
    std::vector<int64_t> efo;
    std::vector efi = {ei, fi};

    auto r = Comm::serverSendAsync(efi, _width, buildTag(_currentMsgTag));
    Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
    Comm::wait(r);

    e = ring(ei ^ efo[0]);
    f = ring(fi ^ efo[1]);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    _zi = (extendedRank & e & f) ^ (f & bmt->_a) ^ (e & bmt->_b) ^ bmt->_c;

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int BoolAndOperator::tagStride(int width) { return BitwiseBmtGenerator::tagStride(width); }

BoolAndOperator* BoolAndOperator::setBmt(BitwiseBmt* bmt) {
    _bmt = bmt;
    return this;
}

} // namespace parsecdb
