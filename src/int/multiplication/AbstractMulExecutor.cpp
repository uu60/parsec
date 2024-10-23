//
// Created by 杜建璋 on 2024/7/13.
//

#include "int/multiplication/AbstractMulExecutor.h"
#include "utils/Mpi.h"
#include "utils/Math.h"
#include "utils/Log.h"

template<typename T>
AbstractMulExecutor<T>::AbstractMulExecutor(T z, bool share) : IntExecutor<T>(z, share) {}

template<typename T>
AbstractMulExecutor<T>::AbstractMulExecutor(T x, T y, bool share) : IntExecutor<T>(x, y, share) {}

template<typename T>
AbstractMulExecutor<T>* AbstractMulExecutor<T>::execute(bool reconstruct) {
    T start, end, end1;
    if (this->_benchmarkLevel >= SecureExecutor<T>::BenchmarkLevel::GENERAL) {
        start = System::currentTimeMillis();
    }
    if (Mpi::isServer()) {
        // MT
        obtainMultiplicationTriple();
        if (this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED) {
            end = System::currentTimeMillis();
            if (this->_isLogBenchmark) {
                Log::i(this->tag() + " Triple computation time: " + std::to_string(end - start) + " ms.");
            }
        }
    }

    // process
    process(reconstruct);
    if (this->_benchmarkLevel >= SecureExecutor<T>::BenchmarkLevel::GENERAL) {
        end1 = System::currentTimeMillis();
        if (this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED && this->_isLogBenchmark) {
            Log::i(this->tag() + " MPI transmission and synchronization time: " + std::to_string(this->_mpiTime) + " ms.");
        }
        if (this->_isLogBenchmark) {
            Log::i(this->tag() + " Entire computation time: " + std::to_string(end1 - start) + " ms.");
        }
        this->_entireComputationTime = end1 - start;
    }

    return this;
}

template<typename T>
void AbstractMulExecutor<T>::process(bool reconstruct) {
    bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
    if (Mpi::isServer()) {
        T ei = this->_xi - _ai;
        T fi = this->_yi - _bi;
        T eo, fo;
        Mpi::sexch(&ei, &eo, this->_mpiTime, detailed);
        Mpi::sexch(&fi, &fo, this->_mpiTime, detailed);
        T e = ei + eo;
        T f = fi + fo;
        this->_zi = Mpi::rank() * e * f + f * _ai + e * _bi + _ci;
        this->_result = this->_zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }
}

template class AbstractMulExecutor<int8_t>;
template class AbstractMulExecutor<int16_t>;
template class AbstractMulExecutor<int32_t>;
template class AbstractMulExecutor<int64_t>;
