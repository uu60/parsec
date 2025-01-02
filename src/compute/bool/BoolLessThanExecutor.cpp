//
// Created by 杜建璋 on 2024/12/29.
//

#include <cmath>

#include "../../../include/compute/bool/BoolLessThanExecutor.h"
#include "../../../include/compute/bool/BoolAndExecutor.h"

BoolLessThanExecutor *BoolLessThanExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        // Compute not(x xor y) which is equality of each bit
        auto x_xor_y = _xi ^ _yi;
        auto equality = IComm::impl->rank() == 0 ? x_xor_y : x_xor_y ^ ((1 << _l) - 1);

        // Compute diagonal
        // Clear lowest bit of
        auto diag = x_xor_y & ~1;
        auto not_y0 = (_yi & 1) ^ IComm::impl->rank();
        diag += not_y0;

        // diag = diag & x
        diag = BoolAndExecutor(diag, _xi, _l, _taskTag, 0, -1).execute()->_zi;
        int rounds = (int) std::floor(std::log2((double) _l));

        // iteration
        auto current = equality;
        for (int r = 0; r <= rounds; r++) {
            int shift_amount = 1 << (r - 1);

            auto shifted = current >> shift_amount;
            // set the highest bit of shifted to 1
            shifted &= (1 << (_l - 1)) - 1;
            shifted ^= (IComm::impl->rank() << (_l - 1));

            current = BoolAndExecutor(current, shifted, _l, _taskTag, 0, -1).execute()->_zi;
        }

        auto shifted = current >> 1;
        // set the highest bit of shifted to 1
        shifted &= (1 << (_l - 1)) - 1;
        shifted ^= (IComm::impl->rank() << (_l - 1));
        current = BoolAndExecutor(shifted, diag, _l, _taskTag, 0, -1).execute()->_zi;

        auto res = current & 1;
        for (int i = 1; i < _l; i++) {
            res ^= (current & (1 << i)) >> i;
        }
        _sign = res;
    }
    return this;
}

BoolLessThanExecutor * BoolLessThanExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_sign, clientRank, buildTag(_currentMsgTag));
    } else {
        bool sign0, sign1;
        IComm::impl->receive(&sign0, 0, buildTag(_currentMsgTag));
        IComm::impl->receive(&sign1, 1, buildTag(_currentMsgTag));
        _result = sign0 ^ sign1;
    }
    return this;
}

std::string BoolLessThanExecutor::className() const {
    return "BoolLessThanExecutor";
}
