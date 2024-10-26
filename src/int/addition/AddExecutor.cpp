//
// Created by 杜建璋 on 2024/7/13.
//

#include "int/addition/AddExecutor.h"
#include "utils/Comm.h"
#include "utils/System.h"
#include <limits>

template<typename T>
AddExecutor<T>::AddExecutor(T z, bool share) : IntExecutor<T>(z, share) {}

template<typename T>
AddExecutor<T>::AddExecutor(T x, T y, bool share) : IntExecutor<T>(x, y, share) {
}

template<typename T>
AddExecutor<T> *AddExecutor<T>::execute(bool reconstruct) {
    bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
    T start;
    if (this->_benchmarkLevel >= SecureExecutor<T>::BenchmarkLevel::GENERAL) {
        start = System::currentTimeMillis();
    }
    if (Comm::isServer()) {
        this->_zi = this->_xi + this->_yi;
        this->_result = this->_zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }
    if (this->_benchmarkLevel >= SecureExecutor<T>::BenchmarkLevel::GENERAL) {
        this->_entireComputationTime = System::currentTimeMillis() - start;
        if (this->_isLogBenchmark) {
            if (detailed) {
                Log::i(tag(),
                       "Comm synchronization and transmission time: " + std::to_string(this->_mpiTime) + " ms.");
            }
            Log::i(tag(), "Entire computation time: " + std::to_string(this->_entireComputationTime) + " ms.");
        }
    }

    return this;
}

template<typename T>
std::string AddExecutor<T>::tag() const {
    return "[Addition Share]";
}

template class AddExecutor<int8_t>;
template class AddExecutor<int16_t>;
template class AddExecutor<int32_t>;
template class AddExecutor<int64_t>;
