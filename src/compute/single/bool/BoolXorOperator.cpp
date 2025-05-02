//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/single/bool/BoolXorOperator.h"

#include "compute/single/arith/ArithAddOperator.h"

BoolXorOperator *BoolXorOperator::execute() {
    _zi = _xi ^ _yi;
    return this;
}

int BoolXorOperator::msgTagCount() {
    return 0;
}
