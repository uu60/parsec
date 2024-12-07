//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/bool/BoolEqualExecutor.h"

#include "compute/bool/BoolAndExecutor.h"
#include "comm/IComm.h"

BoolEqualExecutor *BoolEqualExecutor::execute() {
    if (IComm::impl->isServer()) {
        _zi = _xi ^ _yi;
        // z0 = ~z0
        if (IComm::impl->rank() == 0) {
            _zi = ~_zi;
        }
        // if equal, z will be all 1.
        _sign = _zi & 1;
        for (int i = 0; i < _l - 1; i++) {
            _sign = BoolAndExecutor(_sign, (_zi >> (i + 1)) & 1, 1, _objTag, 0, -1).execute()->_zi;
        }
    }
    return this;
}

BoolEqualExecutor * BoolEqualExecutor::reconstruct(int clientRank) {
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_sign, clientRank, 0);
    } else {
        bool sign0, sign1;
        IComm::impl->receive(&sign0, 0, 0);
        IComm::impl->receive(&sign1, 1, 0);
        _result = sign0 ^ sign1;
    }
    return this;
}

std::string BoolEqualExecutor::className() const {
    return "EqualBoolExecutor";
}