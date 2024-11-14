//
// Created by 杜建璋 on 2024/9/6.
//

#include "bit/BitExecutor.h"
#include "utils/Math.h"
#include "utils/Comm.h"

BitExecutor::BitExecutor(bool z, bool share) {
    if (share) {
        if (Comm::isClient()) {
            bool z1 = Math::randInt(0, 1);
            bool z0 = z ^ z1;
            Comm::send(&z0, 0);
            Comm::send(&z1, 1);
        } else {
            // operator
            Comm::recv(&_zi, Comm::CLIENT_RANK);
        }
    } else {
        this->_zi = z;
    }
}

BitExecutor::BitExecutor(bool x, bool y, bool share) {
    if (share) {
        if (!Comm::isServer()) {
            bool x1 = Math::randInt(0, 1);
            bool x0 = x1 ^ x;
            bool y1 = Math::randInt(0, 1);
            bool y0 = y1 ^ y;
            Comm::send(&x0, 0);
            Comm::send(&y0, 0);
            Comm::send(&x1, 1);
            Comm::send(&y1, 1);
        } else {
            // operator
            Comm::recv(&_xi, Comm::CLIENT_RANK);
            Comm::recv(&_yi, Comm::CLIENT_RANK);
        }
    } else {
        _xi = x;
        _yi = y;
    }
}

BitExecutor *BitExecutor::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&_zi, Comm::CLIENT_RANK);
    } else {
        bool z0, z1;
        Comm::recv(&z0, 0);
        Comm::recv(&z1, 1);
        _result = z0 ^ z1;
    }
    return this;
}

BitExecutor *BitExecutor::execute(bool reconstruct) {
    throw std::runtime_error("This method cannot be called!");
}
std::string BitExecutor::tag() const {
    throw std::runtime_error("This method cannot be called!");
}
