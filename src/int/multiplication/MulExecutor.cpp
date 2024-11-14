//
// Created by 杜建璋 on 2024/7/13.
//

#include "int/multiplication/MulExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"
#include "utils/Log.h"

template<typename T>
MulExecutor<T>::MulExecutor(T x, T y, bool share) : IntArithExecutor<T>(x, y, share) {}

template<typename T>
MulExecutor<T>* MulExecutor<T>::execute(bool reconstruct) {
    // process
    if (Comm::isServer()) {
        T ei = this->_xi - _ai;
        T fi = this->_yi - _bi;
        T eo, fo;
        Comm::sexch(&ei, &eo);
        Comm::sexch(&fi, &fo);
        T e = ei + eo;
        T f = fi + fo;
        this->_zi = Comm::rank() * e * f + f * _ai + e * _bi + _ci;
        this->_result = this->_zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }

    return this;
}

template<typename T>
MulExecutor<T> *MulExecutor<T>::obtainBmt(T ai, T bi, T ci) {
    _ai = ai;
    _bi = bi;
    _ci = ci;
    return this;
}

template<typename T>
void MulExecutor<T>::process(bool reconstruct) {
    if (Comm::isServer()) {
        T ei = this->_xi - _ai;
        T fi = this->_yi - _bi;
        T eo, fo;
        Comm::sexch(&ei, &eo);
        Comm::sexch(&fi, &fo);
        T e = ei + eo;
        T f = fi + fo;
        this->_zi = Comm::rank() * e * f + f * _ai + e * _bi + _ci;
        this->_result = this->_zi;
    }
    if (reconstruct) {
        this->reconstruct();
    }
}

template class MulExecutor<int8_t>;
template class MulExecutor<int16_t>;
template class MulExecutor<int32_t>;
template class MulExecutor<int64_t>;
