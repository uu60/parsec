//
// Created by 杜建璋 on 2024/12/29.
//

#include "../../../include/compute/bool/BoolLessThanExecutor.h"

#include "../../../include/compute/bool/BoolAndExecutor.h"

BoolLessThanExecutor *BoolLessThanExecutor::execute() {
    if (IComm::impl->isServer()) {
        // Step 1: Compute not(x xor y) which is equality of each bit
        auto x_xor_y = _xi ^ _yi;
        auto equality = IComm::impl->rank() == 0 ? x_xor_y : x_xor_y ^ ((1 << _l) - 1);

        // Step 2: Compute diagonal
        // auto diag = BoolAndExecutor
    }
    return this;
}
