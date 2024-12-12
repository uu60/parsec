//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/arith/ArithAddExecutor.h"
#include "comm/IComm.h"
#include "utils/System.h"
#include <limits>

ArithAddExecutor *ArithAddExecutor::execute() {
    if (IComm::impl->isServer()) {
        _zi = ring(_xi + _yi);
    }

    return this;
}

int16_t ArithAddExecutor::neededMsgTags() {
    return 0;
}

std::string ArithAddExecutor::className() const {
    return "AddExecutor";
}
