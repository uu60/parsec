//
// Created by 杜建璋 on 2024/8/29.
//

#include "bit/xor/XorExecutor.h"
#include "utils/Mpi.h"

XorExecutor::XorExecutor(bool z, bool share) : BitExecutor(z, share) {}

XorExecutor::XorExecutor(bool x, bool y, bool share) : BitExecutor(x, y, share) {}

XorExecutor* XorExecutor::execute(bool reconstruct) {
    bool detailed = _benchmarkLevel == BenchmarkLevel::DETAILED;
    int64_t start = System::currentTimeMillis();
    if (Mpi::isServer()) {
        _zi = _xi ^ _yi;
        _result = _zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }
    if (_benchmarkLevel >= SecureExecutor::BenchmarkLevel::GENERAL && _isLogBenchmark) {
        _entireComputationTime = System::currentTimeMillis() - start;
        if (_isLogBenchmark) {
            if (detailed) {
                Log::i(tag(),
                       "Mpi synchronization and transmission time: " + std::to_string(_mpiTime) + " ms.");
            }
            Log::i(tag(), "Entire computation time: " + std::to_string(_entireComputationTime) + " ms.");
        }
    }
    return this;
}

std::string XorExecutor::tag() const {
    return "[XOR Boolean Share]";
}
