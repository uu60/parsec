//
// Created by 杜建璋 on 2024/9/2.
//

#include "int/comparison/ArithLessThanExecutor.h"

#include "bit/BitExecutor.h"
#include "utils/Comm.h"

template<typename T>
ArithLessThanExecutor<T>::ArithLessThanExecutor(T z, bool share) : IntArithExecutor<T>(z, share) {
}

template<typename T>
ArithLessThanExecutor<T>::ArithLessThanExecutor(T x, T y, bool share) : IntArithExecutor<T>(x, y, share) {
    this->_zi = this->_xi - this->_yi;
}

template<typename T>
ArithLessThanExecutor<T> *ArithLessThanExecutor<T>::execute(bool reconstruct) {
    if (Comm::isServer()) {
        this->convertZiToBool();
        _sign = this->_zi >> (this->_l - 1);
    }
    return this;
}

/*
template<typename T>
ArithLessThanExecutor<T> *ArithLessThanExecutor<T>::boolEqual() {
    if (Comm::isServer()) {
        this->_zi = this->_xi ^ this->_yi;
        // z = ~z
        if (Comm::rank() == 0) {
            this->_zi = ~this->_zi;
        }
        // if equal, z will be all 1.
        _sign = this->_zi & 1;
        for (int i = 0; i < this->_l - 1; i++) {
            _sign = BitAndExecutor(_sign, (this->_zi >> (i + 1)) & 1, false).execute()->_zi;
        }
    }
    return this;
}*/

template<typename T>
ArithLessThanExecutor<T> *ArithLessThanExecutor<T>::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&this->_sign, Comm::CLIENT_RANK);
    } else {
        bool sign0, sign1;
        Comm::recv(&sign0, 0);
        Comm::recv(&sign1, 1);
        this->_sign = sign0 ^ sign1;
    }
    return this;
}

template<typename T>
std::string ArithLessThanExecutor<T>::tag() const {
    return "[Comparison]";
}

template class ArithLessThanExecutor<int8_t>;
template class ArithLessThanExecutor<int16_t>;
template class ArithLessThanExecutor<int32_t>;
template class ArithLessThanExecutor<int64_t>;
