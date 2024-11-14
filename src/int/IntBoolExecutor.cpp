//
// Created by 杜建璋 on 2024/11/7.
//

#include "../../include/int/IntBoolExecutor.h"
#include "../../include/utils/Comm.h"
#include "../../include/utils/Math.h"
#include "../../include/ot/RsaOtExecutor.h"
#if __has_include("int/fixed_ab_pairs_0.h")

#include "../../include/int/fixed_ab_pairs_0.h"

#endif

#if __has_include("int/fixed_ab_pairs_1.h")

#include "../../include/int/fixed_ab_pairs_1.h"

#endif

template<class T>
IntBoolExecutor<T>::IntBoolExecutor(T z, bool share) {
    if (share) {
        // distribute operator
        if (Comm::isClient()) {
            T z1 = Math::randInt();
            T z0 = z ^ z1;
            Comm::send(&z0, 0);
            Comm::send(&z1, 1);
        } else {
            // operator
            Comm::recv(&this->_zi, Comm::CLIENT_RANK);
        }
    } else {
        this->_zi = z;
    }
}

template<class T>
IntBoolExecutor<T>::IntBoolExecutor(T x, T y, bool share) {
    if (share) {
        // distribute operator
        if (Comm::isClient()) {
            T x1 = Math::randInt();
            T x0 = x ^ x1;
            T y1 = Math::randInt();
            T y0 = y ^ y1;
            Comm::send(&x0, 0);
            Comm::send(&y0, 0);
            Comm::send(&x1, 1);
            Comm::send(&y1, 1);
        } else {
            // operator
            Comm::recv(&_xi, Comm::CLIENT_RANK);
            Comm::recv(&_yi, Comm::CLIENT_RANK);
        }
    } else {
        _xi = x;
        _yi = y;
    }
}

template<typename T>
T IntBoolExecutor<T>::convertZiToArithmetic(bool ot) {
    if (Comm::isServer()) {
        if (ot) {
            doOtConvert();
        } else {
            doPregeneratedConvert();
        }
    }
    return this->_zi;
}

template<typename T>
void IntBoolExecutor<T>::doOtConvert() {
    bool isSender = Comm::rank() == 0;
    T xa = 0;
    for (int i = 0; i < this->_l; i++) {
        int xb = (this->_zi >> i) & 1;
        T s0 = 0, s1 = 0;
        int64_t r = 0;
        if (isSender) {
            // Sender
            r = Math::randInt() & ((1l << this->_l) - 1);
            s0 = (xb << i) - r;
            s1 = ((1 - xb) << i) - r;
        }
        RsaOtExecutor<T> e(0, s0, s1, xb);
        e.execute(false);
        if (isSender) {
            xa += r;
        } else {
            T s_xb = e._result;
            xa += s_xb;
        }
    }
    this->_zi = xa;
}

template<typename T>
void IntBoolExecutor<T>::doPregeneratedConvert() {
    T res = 0;
    for (int i = 0; i < this->_l; i++) {
        int idx = 0;
        if (Comm::rank() == 0) {
            idx = (int) Math::randInt(0, 99);
            Comm::ssend(&idx);
        } else {
            Comm::srecv(&idx);
        }
        std::pair<T, T> r = getPair(idx);
        T ri_b = r.first;
        T ri_a = r.second;

        // Compute
        T zi_b = ((this->_zi >> i) & 1) ^ ri_b;
        T zo_b;

        // Decrypt
        Comm::sexch(&zi_b, &zo_b);
        T z = zo_b ^ zi_b;
        T zi_a;

        // Compute
        res += (ri_a + z * Comm::rank() - 2 * ri_a * z) << i;
    }
    this->_zi = res;
}

template<typename T>
std::pair<T, T> IntBoolExecutor<T>::getPair(int idx) {
    const std::array<T, 100> *booleans;
    const std::array<T, 100> *ariths;
    if constexpr (std::is_same_v<T, int8_t>) {
        booleans = &R_BOOL_8;
        ariths = &R_ARITH_8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        booleans = &R_BOOL_16;
        ariths = &R_ARITH_16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        booleans = &R_BOOL_32;
        ariths = &R_ARITH_32;
    } else {
        booleans = &R_BOOL_64;
        ariths = &R_ARITH_64;
    }
    return {(*booleans)[idx], (*ariths)[idx]};
}

template<typename T>
IntBoolExecutor<T> *IntBoolExecutor<T>::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&this->_zi, Comm::CLIENT_RANK);
    } else {
        T z0, z1;
        Comm::recv(&z0, 0);
        Comm::recv(&z1, 1);
        this->_result = z0 ^ z1;
    }
    return this;
}

template<typename T>
IntBoolExecutor<T> *IntBoolExecutor<T>::execute(bool reconstruct) {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
std::string IntBoolExecutor<T>::tag() const {
    throw std::runtime_error("This method cannot be called!");
}

template
class IntBoolExecutor<int8_t>;

template
class IntBoolExecutor<int16_t>;

template
class IntBoolExecutor<int32_t>;

template
class IntBoolExecutor<int64_t>;