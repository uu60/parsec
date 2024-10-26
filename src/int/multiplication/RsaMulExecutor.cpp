//
// Created by 杜建璋 on 2024/7/27.
//

#include "int/multiplication/RsaMulExecutor.h"
#include "utils/Comm.h"
#include "bmt/RsaTripleGenerator.h"

template<typename T>
RsaMulExecutor<T>::RsaMulExecutor(T z, bool share): AbstractMulExecutor<T>(z, share) {}

template<typename T>
RsaMulExecutor<T>::RsaMulExecutor(T x, T y, bool share) : AbstractMulExecutor<T>(x, y, share) {}

template<typename T>
void RsaMulExecutor<T>::obtainMultiplicationTriple() {
    RsaTripleGenerator<T> e;
    e.benchmark(this->_benchmarkLevel)->logBenchmark(false)->execute(false);
    if (this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED) {
        this->_mpiTime += e.mpiTime();
    }
    this->_ai = e.ai();
    this->_bi = e.bi();
    this->_ci = e.ci();

    if (this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED && this->_isLogBenchmark) {
        Log::i(tag() + " OT RSA keys generation time: " + std::to_string(e.otRsaGenerationTime()) + " ms.");
        Log::i(tag() + " OT RSA encryption time: " + std::to_string(e.otRsaEncryptionTime()) + " ms.");
        Log::i(tag() + " OT RSA decryption time: " + std::to_string(e.otRsaDecryptionTime()) + " ms.");
        Log::i(tag() + " OT MPI transmission and synchronization time: " + std::to_string(e.otMpiTime()) + " ms.");
        Log::i(tag() + " OT total computation time: " + std::to_string(e.otEntireComputationTime()) + " ms.");
    }
}

template<typename T>
std::string RsaMulExecutor<T>::tag() const {
    return "[RSA OT Multiplication Share]";
}

template class RsaMulExecutor<int8_t>;
template class RsaMulExecutor<int16_t>;
template class RsaMulExecutor<int32_t>;
template class RsaMulExecutor<int64_t>;