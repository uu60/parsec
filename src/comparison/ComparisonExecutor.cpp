//
// Created by 杜建璋 on 2024/9/2.
//

#include "comparison/ComparisonExecutor.h"
#include "utils/Mpi.h"

template<typename T>
ComparisonExecutor<T>::ComparisonExecutor(T z, bool share) : IntExecutor<T>(z, share) {}

template<typename T>
ComparisonExecutor<T>::ComparisonExecutor(T x, T y, bool share) : IntExecutor<T>(x, y, share) {
    this->_zi = this->_xi - this->_yi;
}

template<typename T>
ComparisonExecutor<T> *ComparisonExecutor<T>::execute(bool reconstruct) {
    if (Mpi::isServer()) {
        this->convertZiToBool();
        this->_sign = this->_zi < 0;
    }
    if (reconstruct) {
        this->reconstruct();
    }
    return this;
}

template<typename T>
ComparisonExecutor<T> *ComparisonExecutor<T>::reconstruct() {
    bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
    if (Mpi::isServer()) {
        Mpi::send(&this->_sign, Mpi::CLIENT_RANK, this->_mpiTime, detailed);
    } else {
        bool sign0, sign1;
        Mpi::recv(&sign0, 0, this->_mpiTime, detailed);
        Mpi::recv(&sign1, 1, this->_mpiTime, detailed);
        this->_result = sign0 ^ sign1;
    }
    return this;
}

template<typename T>
std::string ComparisonExecutor<T>::tag() const {
    return "[Comparison]";
}

template<typename T>
bool ComparisonExecutor<T>::sign() {
    return _sign;
}

template class ComparisonExecutor<int8_t>;
template class ComparisonExecutor<int16_t>;
template class ComparisonExecutor<int32_t>;
template class ComparisonExecutor<int64_t>;