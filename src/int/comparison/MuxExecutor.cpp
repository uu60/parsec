//
// Created by 杜建璋 on 2024/10/23.
//

#include "int/comparison/MuxExecutor.h"

#include "api/BitSecret.h"
#include "bit/BitExecutor.h"
#include "bit/and/RsaAndExecutor.h"
#include "int/addition/AddExecutor.h"
#include "int/multiplication/RsaMulExecutor.h"
#include "utils/Comm.h"
template<typename T>
MuxExecutor<T>::MuxExecutor(T x, T y, bool c, bool share) {
    if constexpr (std::is_same_v<T, bool>) {
        auto e = BitExecutor(x, y, share);
        _xi = e.xi();
        _yi = e.yi();
    } else {
        auto e = IntExecutor<T>(x, y, share);
        _xi = e.xi();
        _yi = e.yi();
    }
    if (share) {
        _ci = BitExecutor(c, true).zi();
    } else {
        _ci = c;
    }
}

template<typename T>
MuxExecutor<T> *MuxExecutor<T>::execute(bool reconstruct) {
    if (Comm::isServer()) {
        if constexpr (std::is_same_v<T, bool>) {
            bool cx = RsaAndExecutor(_ci, _xi, false).execute(false)->zi();
            bool cy = RsaAndExecutor(_ci, _yi, false).execute(false)->zi();
            this->_zi = cx ^ this->_yi ^ cy;
        } else {
            auto ci = IntExecutor(static_cast<T>(_ci), false).convertZiToArithmetic(true)->zi();
            T cx = RsaMulExecutor(ci, this->_xi, false).execute(false)->zi();
            T cy = RsaMulExecutor(ci, this->_yi, false).execute(false)->zi();
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
SecureExecutor<T> * MuxExecutor<T>::reconstruct() {
    if constexpr (std::is_same_v<T, bool>) {
        this->_result = BitExecutor(this->_zi, false).reconstruct()->result();
    } else {
        this->_result = IntExecutor<T>(this->_zi, false).reconstruct()->result();
    }
    return this;
}

template class MuxExecutor<bool>;
template class MuxExecutor<int8_t>;
template class MuxExecutor<int16_t>;
template class MuxExecutor<int32_t>;
template class MuxExecutor<int64_t>;


