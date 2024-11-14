//
// Created by 杜建璋 on 2024/7/7.
//

#include "SecureExecutor.h"

template<typename T>
void SecureExecutor<T>::finalize() {
    // do nothing by default
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

template class SecureExecutor<bool>;
template class SecureExecutor<int8_t>;
template class SecureExecutor<int16_t>;
template class SecureExecutor<int32_t>;
template class SecureExecutor<int64_t>;