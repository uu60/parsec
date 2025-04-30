//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/single/arith/ArithAddOperator.h"
#include "comm/Comm.h"
#include "utils/System.h"
#include <limits>

ArithAddOperator *ArithAddOperator::execute() {
    if (Comm::isServer()) {
        _zi = ring(_xi + _yi);
    }

    return this;
}
