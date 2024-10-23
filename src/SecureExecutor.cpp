//
// Created by 杜建璋 on 2024/7/7.
//

#include "SecureExecutor.h"

template<typename T>
void SecureExecutor<T>::finalize() {
    // do nothing by default
}

template<typename T>
T SecureExecutor<T>::result() const {
    return _result;
}

template<typename T>
SecureExecutor<T> *SecureExecutor<T>::benchmark(BenchmarkLevel lv) {
    _benchmarkLevel = lv;
    return this;
}

template<typename T>
int64_t SecureExecutor<T>::mpiTime() const {
    return _mpiTime;
}

template<typename T>
SecureExecutor<T> *SecureExecutor<T>::logBenchmark(bool isLogBenchmark) {
    _isLogBenchmark = isLogBenchmark;
    return this;
}

template<typename T>
int64_t SecureExecutor<T>::entireComputationTime() const {
    return _entireComputationTime;
}

template<typename T>
SecureExecutor<T> *SecureExecutor<T>::reconstruct() {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
SecureExecutor<T> *SecureExecutor<T>::execute(bool reconstruct) {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
std::string SecureExecutor<T>::tag() const {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
T SecureExecutor<T>::zi() {
    return this->_zi;
}

template class SecureExecutor<bool>;
template class SecureExecutor<int8_t>;
template class SecureExecutor<int16_t>;
template class SecureExecutor<int32_t>;
template class SecureExecutor<int64_t>;