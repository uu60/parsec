//
// Created by 杜建璋 on 2024/12/29.
//

#include <cmath>

#include "../../../include/compute/bool/BoolLessExecutor.h"
#include "../../../include/compute/bool/BoolAndExecutor.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolLessExecutor *BoolLessExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    if (IComm::impl->isServer()) {
        int64_t x_xor_y = _xi ^ _yi;
        int64_t not_x_xor_y = IComm::impl->rank() == 0 ? x_xor_y : x_xor_y ^ ((1 << _l) - 1);

        int64_t shifted_1 = shiftGreater(not_x_xor_y, 1);
        int64_t lbs = BoolAndExecutor(not_x_xor_y, shifted_1, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;

        int64_t diag = Math::changeBit(x_xor_y, 0, Math::getBit(_yi, 0) ^ IComm::impl->rank());
        // diag & x
        diag = BoolAndExecutor(diag, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;

        int rounds = (int)std::floor(std::log2(_l));
        for (int r = 2; r <= rounds; r++) {
            int64_t shifted_r = shiftGreater(lbs, r);
            int64_t and_r = BoolAndExecutor(lbs, shifted_r, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;
            lbs = and_r;
        }

        int64_t shifted_accum = Math::changeBit(lbs >> 1, _l - 1, IComm::impl->rank());
        int64_t final_accum = BoolAndExecutor(shifted_accum, diag, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;

        bool result = IComm::impl->rank();
        for (int i = 0; i < _l; i++) {
            result = result ^ Math::getBit(final_accum, i);
        }

        _zi = result;
    }

    return this;
}

BoolLessExecutor *BoolLessExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_zi, clientRank, buildTag(_currentMsgTag));
    } else {
        int64_t sign0, sign1;
        IComm::impl->receive(&sign0, 0, buildTag(_currentMsgTag));
        IComm::impl->receive(&sign1, 1, buildTag(_currentMsgTag));
        _result = sign0 ^ sign1;
    }
    return this;
}

std::string BoolLessExecutor::className() const {
    return "BoolLessThanExecutor";
}

int64_t BoolLessExecutor::shiftGreater(int64_t in, int r) const {
    int part_size = 1 << r;
    if (part_size > _l) {
        return in;
    }

    int offset = part_size >> 1;

    for (int i = 0; i < _l; i += part_size) {
        int start = i + offset;
        if (start >= _l) {
            break;
        }

        bool midBit = Math::getBit(in, start);

        for (int j = start - 1; j >= i; j--) {
            in = Math::changeBit(in, j, midBit);
        }
    }

    return in;
}
