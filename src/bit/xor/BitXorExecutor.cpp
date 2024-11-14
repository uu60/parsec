//
// Created by 杜建璋 on 2024/8/29.
//

#include "bit/xor/BitXorExecutor.h"
#include "utils/Comm.h"

BitXorExecutor::BitXorExecutor(bool z, bool share) : BitExecutor(z, share) {}

BitXorExecutor::BitXorExecutor(bool x, bool y, bool share) : BitExecutor(x, y, share) {}

BitXorExecutor* BitXorExecutor::execute(bool reconstruct) {
    if (Comm::isServer()) {
        _zi = _xi ^ _yi;
        _result = _zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }
    return this;
}

std::string BitXorExecutor::tag() const {
    return "[XOR Boolean Share]";
}
