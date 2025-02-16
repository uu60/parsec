//
// Created by 杜建璋 on 2024/12/29.
//

#include "ot/base/AbstractOtExecutor.h"

AbstractOtExecutor::AbstractOtExecutor(int sender, int64_t m0, int64_t m1, int choice, int l, int16_t taskTag,
                                       int16_t msgTagOffset) : AbstractSingleExecutor(l, taskTag, msgTagOffset) {
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _m0 = ring(m0);
        _m1 = ring(m1);
    } else {
        _choice = choice;
    }
}

AbstractOtExecutor * AbstractOtExecutor::reconstruct(int clientRank) {
    throw std::runtime_error("Should not use reconstruct method on OT.");
}
