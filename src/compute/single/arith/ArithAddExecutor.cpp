//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/single/arith/ArithAddExecutor.h"
#include "comm/Comm.h"
#include "utils/System.h"
#include <limits>

ArithAddExecutor *ArithAddExecutor::execute() {
    if (Comm::isServer()) {
        _zi = ring(_xi + _yi);
    }

    return this;
}
