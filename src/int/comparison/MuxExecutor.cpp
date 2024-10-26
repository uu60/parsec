//
// Created by 杜建璋 on 2024/10/23.
//

#include "int/comparison/MuxExecutor.h"
#include "int/addition/AddExecutor.h"
#include "int/multiplication/RsaMulExecutor.h"
#include "utils/Comm.h"
template<typename T>
MuxExecutor<T>::MuxExecutor(T x, T y, bool c) : IntExecutor<T>(x, y, true) {
    _ci = IntExecutor<T>(c, true).zi();
}

template<typename T>
MuxExecutor<T>::MuxExecutor(T xi, T yi, T ci) : IntExecutor<T>(xi, yi, false) {
    _ci = ci;
}

template<typename T>
MuxExecutor<T> *MuxExecutor<T>::execute(bool reconstruct) {
    if (Comm::isServer()) {
        T cx = RsaMulExecutor(_ci, this->_xi, false).execute(false)->zi();
        T cy = RsaMulExecutor(_ci, this->_yi, false).execute(false)->zi();
        this->_zi = cx + this->_yi - cy;
    }
    if (reconstruct) {
        this->reconstruct();
    }

    return this;
}

template<typename T>
string MuxExecutor<T>::tag() const {
    return "[Mux]";
}

template class MuxExecutor<int8_t>;
template class MuxExecutor<int16_t>;
template class MuxExecutor<int32_t>;
template class MuxExecutor<int64_t>;


