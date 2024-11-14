//
// Created by 杜建璋 on 2024/7/13.
//

#include "int/addition/AddExecutor.h"
#include "utils/Comm.h"
#include "utils/System.h"
#include <limits>

template<typename T>
AddExecutor<T>::AddExecutor(T x, T y, bool share) : IntArithExecutor<T>(x, y, share) {}

template<typename T>
AddExecutor<T> *AddExecutor<T>::execute(bool reconstruct) {
    if (Comm::isServer()) {
        this->_zi = this->_xi + this->_yi;
        this->_result = this->_zi;
    }
    if (reconstruct) {
        this->reconstruct();
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
