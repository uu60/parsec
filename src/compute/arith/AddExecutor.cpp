//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/arith/AddExecutor.h"
#include "utils/Comm.h"
#include "utils/System.h"
#include <limits>

AddExecutor::AddExecutor(int64_t x, int64_t y, int l, bool local) : ArithExecutor(x, y, l, local) {}

AddExecutor *AddExecutor::execute() {
    if (Comm::isServer()) {
        _zi = ring(_xi + _yi);
    }

    return this;
}

std::string AddExecutor::tag() const {
    return "[Addition Share]";
}
