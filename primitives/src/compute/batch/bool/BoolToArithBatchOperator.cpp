//
// Created by 杜建璋 on 25-5-27.
//

#include "compute/batch/bool/BoolToArithBatchOperator.h"

#include "ot/RandOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolToArithBatchOperator *BoolToArithBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    bool isSender = Comm::rank() == 0;

    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;
    std::vector<int64_t> rs;

    size_t n = _width * _xis->size();
    if (isSender) {
        ss0.reserve(n);
        ss1.reserve(n);
        rs.reserve(n);
    } else {
        choices.reserve(n);
    }

    for (int i = 0; i < _xis->size(); i++) {
        for (int j = 0; j < _width; j++) {
            int xb = static_cast<int>(((*_xis)[i] >> j) & 1);
            if (isSender) {
                int64_t r = Math::randInt();
                rs.push_back(r);
                int64_t s0 = (static_cast<int64_t>(xb) << j) - r;
                int64_t s1 = (static_cast<int64_t>(1 - xb) << j) - r;
                ss0.push_back(s0);
                ss1.push_back(s1);
            } else {
                choices.push_back(xb);
            }
        }
    }
    RandOtBatchOperator e(0, &ss0, &ss1, &choices, _width, _taskTag, _currentMsgTag);
    e.execute();

    _zis.resize(_xis->size(), 0);
    if (isSender) {
        for (int i = 0; i < _xis->size(); i++) {
            for (int j = 0; j < _width; j++) {
                _zis[i] = ring(_zis[i] + rs[i * _width + j]);
            }
        }
    } else {
        for (int i = 0; i < _xis->size(); i++) {
            for (int j = 0; j < _width; ++j) {
                _zis[i] = ring(_zis[i] + e._results[i * _width + j]);
            }
        }
    }
    return this;
}

int BoolToArithBatchOperator::tagStride() {
    return RandOtBatchOperator::tagStride();
}
