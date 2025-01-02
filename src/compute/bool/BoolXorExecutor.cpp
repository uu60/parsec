//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/bool/BoolXorExecutor.h"

#include "compute/arith/ArithAddExecutor.h"

BoolXorExecutor *BoolXorExecutor::execute() {
    _zi = _xi ^ _yi;
    return this;
}

int16_t BoolXorExecutor::needsMsgTags() {
    return 0;
}

std::string BoolXorExecutor::className() const {
    return "BitwiseXorExecutor";
}
