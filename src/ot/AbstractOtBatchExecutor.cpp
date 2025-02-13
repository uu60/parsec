//
// Created by 杜建璋 on 2025/1/31.
//

#include "../../include/ot/AbstractOtBatchExecutor.h"

AbstractOtBatchExecutor::AbstractOtBatchExecutor(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1,
                                                 std::vector<int> *choices, int l, int16_t taskTag, int16_t msgTagOffset) : AbstractBatchExecutor(
l, taskTag, msgTagOffset) {
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _ms0 = ms0;
        _ms1 = ms1;
    } else {
        _choices = choices;
    }
}

AbstractOtBatchExecutor *AbstractOtBatchExecutor::reconstruct(int clientRank) {
    throw std::runtime_error("Should not use reconstruct method on OT.");
}
