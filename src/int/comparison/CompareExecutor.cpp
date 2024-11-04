//
// Created by 杜建璋 on 2024/9/2.
//

#include "int/comparison/CompareExecutor.h"
#include "utils/Comm.h"

template<typename T>
CompareExecutor<T>::CompareExecutor(T z, bool share) : IntExecutor<T>(z, share) {}

template<typename T>
CompareExecutor<T>::CompareExecutor(T x, T y, bool share) : IntExecutor<T>(x, y, share) {
    this->_zi = this->_xi - this->_yi;
}

template<typename T>
CompareExecutor<T> *CompareExecutor<T>::execute(bool reconstruct) {
    if (Comm::isServer()) {
        this->convertZiToBool();
        this->_sign = this->_zi < 0;
    }
    if (reconstruct) {
        this->reconstruct();
    }
    return this;
}

template<typename T>
CompareExecutor<T> *CompareExecutor<T>::reconstruct() {
    bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
    if (Comm::isServer()) {
        Comm::send(&this->_sign, Comm::CLIENT_RANK, this->_mpiTime, detailed);
    } else {
        bool sign0, sign1;
        Comm::recv(&sign0, 0, this->_mpiTime, detailed);
        Comm::recv(&sign1, 1, this->_mpiTime, detailed);
        this->_result = sign0 ^ sign1;
    }
    return this;
}

template<typename T>
std::string CompareExecutor<T>::tag() const {
    return "[Comparison]";
}

template class CompareExecutor<int8_t>;
template class CompareExecutor<int16_t>;
template class CompareExecutor<int32_t>;
template class CompareExecutor<int64_t>;