//
// Created by 杜建璋 on 2024/12/1.
//

#include "compute/arith/ArithToBoolExecutor.h"

#include "comm/IComm.h"
#include "compute/bool/BoolAndExecutor.h"
#include "utils/Math.h"

ArithToBoolExecutor *ArithToBoolExecutor::execute() {
    int64_t b_zi = 0;

    if (IComm::impl->isServer()) {
        // bitwise separate xi
        // xi is xored into xi_i and xi_o
        int64_t xi_i = ring(Math::randInt());
        int64_t xi_o = ring(xi_i ^ _xi);
        bool carry_i = false;

        auto objTags = nextMsgTags(_l * 4);

        for (int i = 0; i < _l; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = IComm::impl->rank() == 0 ? &ai : &bi;
            bool *self_o = IComm::impl->rank() == 0 ? &ao : &bo;
            bool *other_i = IComm::impl->rank() == 0 ? &bi : &ai;
            *self_i = (xi_i >> i) & 1;
            *self_o = (xi_o >> i) & 1;
            IComm::impl->serverExchange(self_o, other_i, buildTag(objTags[i * 4]));
            b_zi += ((ai ^ bi) ^ carry_i) << i;

            // Compute carry_i
            bool generate_i = BoolAndExecutor(ai, bi, 1, _objTag, _currentMsgTag, -1).execute()->_zi;
            bool propagate_i = ai ^ bi;
            bool tempCarry_i = BoolAndExecutor(propagate_i, carry_i, 1, _objTag, _currentMsgTag, -1).execute()->_zi;
            bool sum_i = generate_i ^ tempCarry_i;
            bool and_i = BoolAndExecutor(generate_i, tempCarry_i, 1, _objTag, _currentMsgTag, -1).execute()->
                    _zi;

            carry_i = sum_i ^ and_i;
        }
    }

    return this;
}

std::string ArithToBoolExecutor::className() const {
    return "ArithToBoolExecutor";
}

int16_t ArithToBoolExecutor::neededMsgTags() {
    return 1;
}
