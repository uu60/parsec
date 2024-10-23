//
// Created by 杜建璋 on 2024/8/30.
//

#include "bmt/RsaTripleGenerator.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Math.h"
#include "utils/Mpi.h"

template<typename T>
RsaTripleGenerator<T>::RsaTripleGenerator() = default;

template<typename T>
void RsaTripleGenerator<T>::generateRandomAB() {
    if (_boolType) {
        this->_ai = Math::rand32(0, 1);
        this->_bi = Math::rand32(0, 1);
        return;
    }
    this->_ai = Math::rand64();
    this->_bi = Math::rand64();
}

template<typename T>
void RsaTripleGenerator<T>::computeU() {
    computeMix(0, this->_ui);
}

template<typename T>
void RsaTripleGenerator<T>::computeV() {
    computeMix(1, this->_vi);
}

template<typename T>
T RsaTripleGenerator<T>::corr(int i, T x) const {
    if (_boolType) {
        return ((this->_ai << i) - x) & 1;
    }
    return (this->_ai << i) - x;
}

template<typename T>
void RsaTripleGenerator<T>::computeMix(int sender, T &mix) {
    bool isSender = Mpi::rank() == sender;
    T sum = 0;
    for (int i = 0; i < this->_l; i++) {
        T s0 = 0, s1 = 0;
        int choice = 0;
        if (isSender) {
            s0 = _boolType ? Math::rand32(0, 1) : Math::rand64();
            s1 = corr(i, s0);
        } else {
            choice = (int) ((this->_bi >> i) & 1);
        }
        RsaOtExecutor<T> r(sender, s0, s1, choice);
        r.logBenchmark(false);
        if (this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED) {
            r.benchmark(SecureExecutor<T>::BenchmarkLevel::DETAILED);
        }
        r.execute(false);

        if (this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED) {
            // add mpi time
            this->_mpiTime += r.mpiTime();
            _otMpiTime += r.mpiTime();
            _otRsaGenerationTime += r.rsaGenerationTime();
            _otRsaEncryptionTime += r.rsaEncryptionTime();
            _otRsaDecryptionTime += r.rsaDecryptionTime();
            _otEntireComputationTime += r.entireComputationTime();
        }

        if (isSender) {
            sum = _boolType ? sum ^ s0 : sum + s0;
        } else {
            T temp = r.result();
            if (choice == 0) {
                temp = _boolType ? temp : -temp;
            }
            sum = _boolType ? sum ^ temp : sum + temp;
        }
    }
    mix = sum;
}

template<typename T>
void RsaTripleGenerator<T>::computeC() {
    if (_boolType) {
        this->_ci = this->_ai & this->_bi ^ this->_ui ^ this->_vi;
        return;
    }
    this->_ci = this->_ai * this->_bi + this->_ui + this->_vi;
}

template<typename T>
RsaTripleGenerator<T> *RsaTripleGenerator<T>::execute(bool dummy) {
    generateRandomAB();

    computeU();
    computeV();
    computeC();
    return this;
}

template<typename T>
int64_t RsaTripleGenerator<T>::otRsaGenerationTime() const {
    return _otRsaGenerationTime;
}

template<typename T>
int64_t RsaTripleGenerator<T>::otRsaEncryptionTime() const {
    return _otRsaEncryptionTime;
}

template<typename T>
int64_t RsaTripleGenerator<T>::otRsaDecryptionTime() const {
    return _otRsaDecryptionTime;
}

template<typename T>
int64_t RsaTripleGenerator<T>::otMpiTime() const {
    return _otMpiTime;
}

template<typename T>
int64_t RsaTripleGenerator<T>::otEntireComputationTime() const {
    return _otEntireComputationTime;
}

template<typename T>
std::string RsaTripleGenerator<T>::tag() const {
    return "[BMT Generator]";
}

template class RsaTripleGenerator<bool>;
template class RsaTripleGenerator<int8_t>;
template class RsaTripleGenerator<int16_t>;
template class RsaTripleGenerator<int32_t>;
template class RsaTripleGenerator<int64_t>;