//
// Created by 杜建璋 on 2024/10/23.
//

#include "int/comparison/MuxExecutor.h"

#include "api/BitSecret.h"
#include "bit/BitExecutor.h"
#include "bit/and/BitAndExecutor.h"
#include "int/addition/AddExecutor.h"
#include "int/multiplication/MulExecutor.h"
#include "utils/Comm.h"

template<typename T>
MuxExecutor<T>::MuxExecutor(T x, T y, bool c, bool share) {
    if constexpr (std::is_same_v<T, bool>) {
        auto e = BitExecutor(x, y, share);
        _xi = e._xi;
        _yi = e._yi;
    } else {
        auto e = IntArithExecutor<T>(x, y, share);
        _xi = e._xi;
        _yi = e._yi;
    }
    if (share) {
        _cond_i = BitExecutor(c, true)._zi;
    } else {
        _cond_i = c;
    }
}

template<typename T>
MuxExecutor<T> *MuxExecutor<T>::execute(bool reconstruct) {
    if (Comm::isServer()) {
        if constexpr (std::is_same_v<T, bool>) {
            bool cx = BitAndExecutor(_cond_i, _xi, false).obtainBmt(_ai, _bi, _ci)->execute(false)->_zi;
            bool cy = BitAndExecutor(_cond_i, _yi, false).obtainBmt(_ai, _bi, _ci)->execute(false)->_zi;
            this->_zi = cx ^ this->_yi ^ cy;
        } else {
            auto a_cond_i = IntBoolExecutor(static_cast<T>(_cond_i), false).convertZiToArithmetic(true);
            T cx = MulExecutor(a_cond_i, this->_xi, false).obtainBmt(_ai, _bi, _ci)->execute(false)->_zi;
            T cy = MulExecutor(a_cond_i, this->_yi, false).obtainBmt(_ai, _bi, _ci)->execute(false)->_zi;
            this->_zi = cx + this->_yi - cy;
        }
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

template<typename T>
SecureExecutor<T> *MuxExecutor<T>::reconstruct() {
    if constexpr (std::is_same_v<T, bool>) {
        this->_result = BitExecutor(this->_zi, false).reconstruct()->_result;
    } else {
        this->_result = IntArithExecutor<T>(this->_zi, false).reconstruct()->_result;
    }
    return this;
}

template<typename T>
MuxExecutor<T> *MuxExecutor<T>::obtainBmt(T ai, T bi, T ci) {
    _ai = ai;
    _bi = bi;
    _ci = ci;
    return this;
}

template class MuxExecutor<bool>;
template class MuxExecutor<int8_t>;
template class MuxExecutor<int16_t>;
template class MuxExecutor<int32_t>;
template class MuxExecutor<int64_t>;
