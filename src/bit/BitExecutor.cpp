//
// Created by 杜建璋 on 2024/9/6.
//

#include "bit/BitExecutor.h"
#include "utils/Math.h"
#include "utils/Comm.h"

BitExecutor::BitExecutor(bool z, bool share) {
    if (share) {
        bool detailed = _benchmarkLevel == BenchmarkLevel::DETAILED;
        if (Comm::isClient()) {
            bool z1 = Math::randInt(0, 1);
            bool z0 = z ^ z1;
            Comm::send(&z0, 0, _mpiTime, detailed);
            Comm::send(&z1, 1, _mpiTime, detailed);
        } else {
            // operator
            Comm::recv(&_zi, Comm::CLIENT_RANK, _mpiTime, detailed);
        }
    } else {
        this->_zi = z;
    }
}

BitExecutor::BitExecutor(bool x, bool y, bool share) {
    if (share) {
        bool detailed = _benchmarkLevel == BenchmarkLevel::DETAILED;
        if (!Comm::isServer()) {
            bool x1 = Math::randInt(0, 1);
            bool x0 = x1 ^ x;
            bool y1 = Math::randInt(0, 1);
            bool y0 = y1 ^ y;
            Comm::send(&x0, 0, _mpiTime, detailed);
            Comm::send(&y0, 0, _mpiTime, detailed);
            Comm::send(&x1, 1, _mpiTime, detailed);
            Comm::send(&y1, 1, _mpiTime, detailed);
        } else {
            // operator
            Comm::recv(&_xi, Comm::CLIENT_RANK, _mpiTime, detailed);
            Comm::recv(&_yi, Comm::CLIENT_RANK, _mpiTime, detailed);
        }
    } else {
        _xi = x;
        _yi = y;
    }
}

BitExecutor *BitExecutor::reconstruct() {
    bool detailed = _benchmarkLevel == SecureExecutor::BenchmarkLevel::DETAILED;
    if (Comm::isServer()) {
        Comm::send(&_zi, Comm::CLIENT_RANK, _mpiTime, detailed);
    } else {
        bool z0, z1;
        Comm::recv(&z0, 0, _mpiTime, detailed);
        Comm::recv(&z1, 1, _mpiTime, detailed);
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
