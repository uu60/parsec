//
// Created by 杜建璋 on 2024/9/6.
//

#include "compute/ArithExecutor.h"
#include "compute/bool/BitwiseAndExecutor.h"
#include "compute/arith/MulExecutor.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"

ArithExecutor::ArithExecutor(int64_t z, int l, bool local) : SecureExecutor(l) {
    if (local) {
        _zi = ring(z);
    } else {
        // distribute operator
        if (Comm::isClient()) {
            int64_t z1 = ring(Math::randInt());
            int64_t z0 = ring(z - z1);
            Comm::send(&z0, 0);
            Comm::send(&z1, 1);
        } else {
            // operator
            Comm::recv(&_zi, Comm::CLIENT_RANK);
        }
    }
}

ArithExecutor::ArithExecutor(int64_t x, int64_t y, int l, bool local) : SecureExecutor(l) {
    if (local) {
        _xi = ring(x);
        _yi = ring(y);
    } else {
        // distribute operator
        if (Comm::isClient()) {
            int64_t x1 = ring(Math::randInt());
            int64_t x0 = ring(x - x1);
            int64_t y1 = ring(Math::randInt());
            int64_t y0 = ring(y - y1);
            Comm::send(&x0, 0);
            Comm::send(&y0, 0);
            Comm::send(&x1, 1);
            Comm::send(&y1, 1);
        } else {
            // operator
            Comm::recv(&_xi, Comm::CLIENT_RANK);
            Comm::recv(&_yi, Comm::CLIENT_RANK);
        }
    }
}

ArithExecutor *ArithExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string ArithExecutor::tag() const {
    throw std::runtime_error("This method cannot be called!");
}

ArithExecutor *ArithExecutor::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&_zi, Comm::CLIENT_RANK);
    } else {
        int64_t z0, z1;
        Comm::recv(&z0, 0);
        Comm::recv(&z1, 1);
        _result = ring(z0 + z1);
    }
    return this;
}

int64_t ArithExecutor::boolZi(std::vector<BMT> bmts) const {
    int64_t b_zi = 0;

    if (Comm::isServer()) {
        // bitwise separate zi
        // zi is xor shared into zi_i and zi_o
        int64_t zi_i = ring(Math::randInt());
        int64_t zi_o = ring(zi_i ^ _zi);
        bool carry_i = false;

        for (int i = 0; i < _l; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = Comm::rank() == 0 ? &ai : &bi;
            bool *self_o = Comm::rank() == 0 ? &ao : &bo;
            bool *other_i = Comm::rank() == 0 ? &bi : &ai;
            *self_i = (zi_i >> i) & 1;
            *self_o = (zi_o >> i) & 1;
            Comm::sexch(self_o, other_i);
            b_zi += ((ai ^ bi) ^ carry_i) << i;

            // Compute carry_i
            bool generate_i = BitwiseAndExecutor(ai, bi, 1, true).setBmts({bmts[3 * i]})->execute()->_zi;
            bool propagate_i = ai ^ bi;
            bool tempCarry_i = BitwiseAndExecutor(propagate_i, carry_i, 1, true).setBmts({bmts[3 * i + 1]})->execute()->_zi;
            bool sum_i = generate_i ^ tempCarry_i;
            bool and_i = BitwiseAndExecutor(generate_i, tempCarry_i, 1, true).setBmts({bmts[3 * i + 2]})->execute()->_zi;

            carry_i = sum_i ^ and_i;
        }
    }

    return b_zi;
}
