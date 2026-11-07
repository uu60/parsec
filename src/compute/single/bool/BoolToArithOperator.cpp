#include "parsec/compute/single/bool/BoolToArithOperator.h"

#include "parsec/comm/Comm.h"
#include "parsec/compute/single/arith/ArithOperator.h"
#include "parsec/intermediate/IntermediateDataSupport.h"
#include "parsec/ot/IknpOtBatchOperator.h"
#include "parsec/parallel/ThreadPoolSupport.h"
#include "parsec/utils/Math.h"

#include <algorithm>

namespace parsec {

BoolToArithOperator* BoolToArithOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t temp = 0;
    const int sender = (_startMsgTag / IknpOtBatchOperator::tagStride()) & 1;
    const bool isSender = Comm::rank() == sender;

    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;
    std::vector<int64_t> rs;

    if (isSender) {
        ss0.reserve(_width);
        ss1.reserve(_width);
        rs.reserve(_width);
    } else {
        choices.reserve(_width);
    }

    for (int i = 0; i < _width; i++) {
        int xb = static_cast<int>(Math::getBit(_xi, i));
        if (isSender) {
            int64_t r = Math::randBits();
            rs.push_back(r);
            int64_t s0 = Math::ringSubtract(Math::changeBit(0, i, xb != 0), r, _width);
            int64_t s1 = Math::ringSubtract(Math::changeBit(0, i, xb == 0), r, _width);
            ss0.push_back(s0);
            ss1.push_back(s1);
        } else {
            choices.push_back(xb);
        }
    }

    IknpOtBatchOperator e(sender, &ss0, &ss1, &choices, _width, _taskTag, _currentMsgTag);
    e.execute();

    if (isSender) {
        for (auto r : rs) {
            temp = Math::ringAdd(temp, r, _width);
        }
    } else {
        for (int i = 0; i < _width; ++i) {
            temp = Math::ringAdd(temp, e._results[i], _width);
        }
    }

    _zi = temp;
    return this;
}

// One IKNP execution, which needs IknpOtBatchOperator::tagStride() tags. Callers historically reserved `width` tags, so
// keep that for wider inputs and only raise it where it was too small (width 1).
int BoolToArithOperator::tagStride(int width) { return std::max(width, IknpOtBatchOperator::tagStride()); }

BoolToArithOperator* BoolToArithOperator::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    ArithOperator e(_zi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (Comm::rank() == clientRank) {
        _result = e._result;
    }
    return this;
}

} // namespace parsec
