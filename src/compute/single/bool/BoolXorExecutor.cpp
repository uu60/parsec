//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/single/bool/BoolXorExecutor.h"

#include "compute/single/arith/ArithAddExecutor.h"

BoolXorExecutor *BoolXorExecutor::execute() {
    _zi = _xi ^ _yi;
    return this;
}

int BoolXorExecutor::msgTagCount() {
    return 0;
}
