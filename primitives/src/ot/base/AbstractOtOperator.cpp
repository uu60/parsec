
#include "ot/base/AbstractOtOperator.h"

AbstractOtOperator::AbstractOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag,
                                       int msgTagOffset) : AbstractSingleOperator(l, taskTag, msgTagOffset) {
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _m0 = ring(m0);
        _m1 = ring(m1);
    } else {
        _choice = choice;
    }
}

AbstractOtOperator * AbstractOtOperator::reconstruct(int clientRank) {
    throw std::runtime_error("Should not use reconstruct method on OT.");
}
