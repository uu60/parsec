//
// Created by 杜建璋 on 2024/9/2.
//

#include "compute/arith/LessThanArithExecutor.h"

#include <utility>

#include "utils/Comm.h"

LessThanArithExecutor::LessThanArithExecutor(int64_t z, int l, bool local) : ArithExecutor(z, l, local) {
}

LessThanArithExecutor::LessThanArithExecutor(int64_t x, int64_t y, int l, bool local) : ArithExecutor(x, y, l, local) {
    _zi = ring(_xi - _yi);
}

LessThanArithExecutor * LessThanArithExecutor::setBmts(std::vector<BMT> bmts) {
    _bmts = std::move(bmts);
    return this;
}

LessThanArithExecutor *LessThanArithExecutor::execute() {
    if (Comm::isServer()) {
        _zi = boolZi(_bmts);
        _sign = (_zi >> (_l - 1)) & 1;
    }
    return this;
}

LessThanArithExecutor *LessThanArithExecutor::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&_sign, Comm::CLIENT_RANK);
    } else {
        bool sign0, sign1;
        Comm::recv(&sign0, 0);
        Comm::recv(&sign1, 1);
        _result = sign0 ^ sign1;
    }
    return this;
}

std::string LessThanArithExecutor::tag() const {
    return "[ArithLessThanExecutor]";
}