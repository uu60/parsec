//
// Created by 杜建璋 on 2024/7/27.
//

#include "int/multiplication/FixedMulExecutor.h"
#include "bmt/FixedTripleGenerator.h"
#include "utils/Mpi.h"

template<typename T>
FixedMulExecutor<T>::FixedMulExecutor(T z, bool share) : AbstractMulExecutor<T>(z, share) {}

template<typename T>
FixedMulExecutor<T>::FixedMulExecutor(T x, T y, bool share) : AbstractMulExecutor<T>(x, y, share) {}

template<typename T>
void FixedMulExecutor<T>::obtainMultiplicationTriple() {
    FixedTripleGenerator<T> e;
    e.benchmark(this->_benchmarkLevel);
    e.logBenchmark(false);
    e.execute(false);

    this->_ai = e.ai();
    this->_bi = e.bi();
    this->_ci = e.ci();
}

template<typename T>
std::string FixedMulExecutor<T>::tag() const {
    return "[Fixed Multiplication Share]";
}

template class FixedMulExecutor<int8_t>;
template class FixedMulExecutor<int16_t>;
template class FixedMulExecutor<int32_t>;
template class FixedMulExecutor<int64_t>;


