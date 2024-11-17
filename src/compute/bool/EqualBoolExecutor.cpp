//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/bool/EqualBoolExecutor.h"

#include "compute/bool/BitwiseAndExecutor.h"
#include "utils/Comm.h"

EqualBoolExecutor *EqualBoolExecutor::execute() {
    if (Comm::isServer()) {
        _zi = _xi ^ _yi;
        // z = ~z
        if (Comm::rank() == 0) {
            _zi = ~_zi;
        }
        // if equal, z will be all 1.
        _sign = _zi & 1;
        for (int i = 0; i < _l - 1; i++) {
            _sign = BitwiseAndExecutor(_sign, (_zi >> (i + 1)) & 1, 1, true).execute()->_zi;
        }
    }
    return this;
}

EqualBoolExecutor * EqualBoolExecutor::reconstruct() {
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

std::string EqualBoolExecutor::tag() const {
    return "[BoolEqualExecutor]";
}