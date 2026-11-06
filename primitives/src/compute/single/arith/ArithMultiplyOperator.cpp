
#include "compute/single/arith/ArithMultiplyOperator.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/Comm.h"
#include "intermediate/BmtGenerator.h"
#include "utils/Log.h"
#include "utils/Math.h"

namespace parsec {


ArithMultiplyOperator *ArithMultiplyOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        Bmt bmt;
        if (_bmt != nullptr) {
            bmt = *_bmt;
        } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
            bmt = IntermediateDataSupport::pollBmts(1, _width)[0];
        } else {
            bmt = BmtGenerator(_width, _taskTag, _currentMsgTag).execute()->_bmt;
        }
        int64_t ei = Math::ringSubtract(_xi, bmt._a, _width);
        int64_t fi = Math::ringSubtract(_yi, bmt._b, _width);
        std::vector efi = {ei, fi};
        std::vector<int64_t> efo;
        Comm::serverSend(efi, _width, buildTag(_currentMsgTag));
        Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
        int64_t e = Math::ringAdd(ei, efo[0], _width);
        int64_t f = Math::ringAdd(fi, efo[1], _width);
        const int64_t ef = Comm::rank() == 0 ? 0 : Math::ringMultiply(e, f, _width);
        const int64_t fa = Math::ringMultiply(f, bmt._a, _width);
        const int64_t eb = Math::ringMultiply(e, bmt._b, _width);
        _zi = Math::ringAdd(Math::ringAdd(ef, fa, _width), Math::ringAdd(eb, bmt._c, _width), _width);
    }

    return this;
}

int ArithMultiplyOperator::tagStride(int width) {
    return BmtGenerator::tagStride(width);
}

ArithMultiplyOperator *ArithMultiplyOperator::setBmt(Bmt *bmt) {
    _bmt = bmt;
    return this;
}

} // namespace parsec
