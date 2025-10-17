
#include "compute/single/bool/BoolToArithOperator.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/Comm.h"
#include "compute/single/arith/ArithOperator.h"
#include "ot/RandOtBatchOperator.h"
#include "ot/RandOtOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

BoolToArithOperator *BoolToArithOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    std::atomic_int64_t temp = 0;
    bool isSender = Comm::rank() == 0;

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
        int xb = static_cast<int>((_xi >> i) & 1);
        if (isSender) {
            int64_t r = Math::randInt();
            rs.push_back(r);
            int64_t s0 = (static_cast<int64_t>(xb) << i) - r;
            int64_t s1 = (static_cast<int64_t>(1 - xb) << i) - r;
            ss0.push_back(s0);
            ss1.push_back(s1);
        } else {
            choices.push_back(xb);
        }
    }

    RandOtBatchOperator e(0, &ss0, &ss1, &choices, _width, _taskTag, _currentMsgTag);
    e.execute();

    if (isSender) {
        for (auto r: rs) {
            temp += r;
        }
    } else {
        for (int i = 0; i < _width; ++i) {
            temp += e._results[i];
        }
    }

    _zi = ring(temp);
    return this;
}

int BoolToArithOperator::tagStride(int width) {
    return RandOtOperator::tagStride(width) * width;
}

BoolToArithOperator *BoolToArithOperator::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    ArithOperator e(_zi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (Comm::rank() == clientRank) {
        _result = e._result;
    }
    return this;
}


