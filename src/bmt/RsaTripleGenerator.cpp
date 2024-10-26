//
// Created by 杜建璋 on 2024/8/30.
//

#include "bmt/RsaTripleGenerator.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Math.h"
#include "utils/Comm.h"

template<typename T>
RsaTripleGenerator<T>::RsaTripleGenerator() = default;

template<typename T>
void RsaTripleGenerator<T>::generateRandomAB() {
    this->_ai = Math::randInt() & this->_mask;
    this->_bi = Math::randInt() & this->_mask;
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
    return ((this->_ai << i) - x) & this->_mask;
}

template<typename T>
void RsaTripleGenerator<T>::computeMix(int sender, T &mix) {
    bool isSender = Comm::rank() == sender;
    T sum = 0;
    for (int i = 0; i < this->_l; i++) {
        T s0 = 0, s1 = 0;
        int choice = 0;
        if (isSender) {
            s0 = Math::randInt(0, 1) & this->_mask;
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
            sum = sum + s0;
        } else {
            T temp = r.result();
            if (choice == 0) {
                temp = (-temp) & this->_mask;
            }
            sum = sum + temp;
        }
    }
    mix = sum;
}

template<typename T>
void RsaTripleGenerator<T>::computeC() {
    if (this->_l == 1) {
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