//
// Created by 杜建璋 on 2024/9/6.
//

#include "bit/BitExecutor.h"
#include "utils/Math.h"
#include "utils/Mpi.h"

BitExecutor::BitExecutor(bool z, bool share) {
    if (share) {
        bool detailed = _benchmarkLevel == BenchmarkLevel::DETAILED;
        if (!Mpi::isServer()) {
            bool z1 = Math::rand32(0, 1);
            bool z0 = z ^ z1;
            Mpi::send(&z0, 0, _mpiTime, detailed);
            Mpi::send(&z1, 1, _mpiTime, detailed);
        } else {
            // operator
            Mpi::recv(&_zi, Mpi::CLIENT_RANK, _mpiTime, detailed);
            Mpi::recv(&_zi, Mpi::CLIENT_RANK, _mpiTime, detailed);
        }
    } else {
        this->_zi = z;
    }
}

BitExecutor::BitExecutor(bool x, bool y, bool share) {
    if (share) {
        bool detailed = _benchmarkLevel == BenchmarkLevel::DETAILED;
        if (!Mpi::isServer()) {
            bool x1 = Math::rand32(0, 1);
            bool x0 = x1 ^ x;
            bool y1 = Math::rand32(0, 1);
            bool y0 = y1 ^ y;
            Mpi::send(&x0, 0, _mpiTime, detailed);
            Mpi::send(&y0, 0, _mpiTime, detailed);
            Mpi::send(&x1, 1, _mpiTime, detailed);
            Mpi::send(&y1, 1, _mpiTime, detailed);
        } else {
            // operator
            Mpi::recv(&_xi, Mpi::CLIENT_RANK, _mpiTime, detailed);
            Mpi::recv(&_yi, Mpi::CLIENT_RANK, _mpiTime, detailed);
        }
    } else {
        _xi = x;
        _yi = y;
    }
}

BitExecutor *BitExecutor::reconstruct() {
    bool detailed = _benchmarkLevel == SecureExecutor::BenchmarkLevel::DETAILED;
    if (Mpi::isServer()) {
        Mpi::send(&_zi, Mpi::CLIENT_RANK, _mpiTime, detailed);
    } else {
        bool z0, z1;
        Mpi::recv(&z0, 0, _mpiTime, detailed);
        Mpi::recv(&z1, 1, _mpiTime, detailed);
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
