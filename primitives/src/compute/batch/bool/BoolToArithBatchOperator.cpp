#include "compute/batch/bool/BoolToArithBatchOperator.h"

#include "ot/IknpOtBatchOperator.h"
#include "ot/RandOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/Math.h"

namespace parsec {

BoolToArithBatchOperator* BoolToArithBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    const int sender = (_startMsgTag / IknpOtBatchOperator::tagStride()) & 1;
    const bool isSender = Comm::rank() == sender;

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
            int xb = static_cast<int>(Math::getBit((*_xis)[i], j));
            if (isSender) {
                int64_t r = Math::randBits();
                rs.push_back(r);
                int64_t s0 = Math::ringSubtract(Math::changeBit(0, j, xb != 0), r, _width);
                int64_t s1 = Math::ringSubtract(Math::changeBit(0, j, xb == 0), r, _width);
                ss0.push_back(s0);
                ss1.push_back(s1);
            } else {
                choices.push_back(xb);
            }
        }
    }
    IknpOtBatchOperator e(sender, &ss0, &ss1, &choices, _width, _taskTag, _currentMsgTag);
    e.execute();

    _zis.resize(_xis->size(), 0);
    if (isSender) {
        for (int i = 0; i < _xis->size(); i++) {
            for (int j = 0; j < _width; j++) {
                _zis[i] = Math::ringAdd(_zis[i], rs[i * _width + j], _width);
            }
        }
    } else {
        for (int i = 0; i < _xis->size(); i++) {
            for (int j = 0; j < _width; ++j) {
                _zis[i] = Math::ringAdd(_zis[i], e._results[i * _width + j], _width);
            }
        }
    }
    return this;
}

int BoolToArithBatchOperator::tagStride() { return IknpOtBatchOperator::tagStride(); }

} // namespace parsec
