//
// Created by 杜建璋 on 2024/9/6.
//

#include "int/IntExecutor.h"
#include "int/addition/AddExecutor.h"
#include "bit/and/RsaAndExecutor.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"

#if __has_include("int/fixed_ab_pairs_0.h")

#include "int/fixed_ab_pairs_0.h"

#endif

#if __has_include("int/fixed_ab_pairs_1.h")

#include "int/fixed_ab_pairs_1.h"

#endif

#include <utility>
#include <array>

template<typename T>
IntExecutor<T>::IntExecutor(T z, bool share) {
    if (share) {
        bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
        // distribute operator
        if (Comm::isClient()) {
            T z1 = Math::randInt();
            T z0 = z - z1;
            Comm::send(&z0, 0, this->_mpiTime, detailed);
            Comm::send(&z1, 1, this->_mpiTime, detailed);
        } else {
            // operator
            Comm::recv(&this->_zi, Comm::CLIENT_RANK, this->_mpiTime, detailed);
        }
    } else {
        this->_zi = z;
    }
}

template<typename T>
IntExecutor<T>::IntExecutor(T x, T y, bool share) {
    if (share) {
        bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
        // distribute operator
        if (Comm::isClient()) {
            T x1 = Math::randInt();
            T x0 = x - x1;
            T y1 = Math::randInt();
            T y0 = y - y1;
            Comm::send(&x0, 0, this->_mpiTime, detailed);
            Comm::send(&y0, 0, this->_mpiTime, detailed);
            Comm::send(&x1, 1, this->_mpiTime, detailed);
            Comm::send(&y1, 1, this->_mpiTime, detailed);
        } else {
            // operator
            Comm::recv(&_xi, Comm::CLIENT_RANK, this->_mpiTime, detailed);
            Comm::recv(&_yi, Comm::CLIENT_RANK, this->_mpiTime, detailed);
        }
    } else {
        _xi = x;
        _yi = y;
    }
}

template<typename T>
IntExecutor<T> *IntExecutor<T>::execute(bool reconstruct) {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
std::string IntExecutor<T>::tag() const {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
IntExecutor<T> *IntExecutor<T>::reconstruct() {
    bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
    if (Comm::isServer()) {
        Comm::send(&this->_zi, Comm::CLIENT_RANK, this->_mpiTime, detailed);
    } else {
        T z0, z1;
        Comm::recv(&z0, 0, this->_mpiTime, detailed);
        Comm::recv(&z1, 1, this->_mpiTime, detailed);
        this->_result = z0 + z1;
    }
    return this;
}

template<typename T>
IntExecutor<T> *IntExecutor<T>::convertZiToBool() {
    if (Comm::isServer()) {
        // bitwise separate zi
        // zi is xor shared into zi_i and zi_o
        T zi_i = Math::randInt();
        T zi_o = zi_i ^ this->_zi;
        this->_zi = 0;
        bool carry_i = false;

        for (int i = 0; i < this->_l; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = Comm::rank() == 0 ? &ai : &bi;
            bool *self_o = Comm::rank() == 0 ? &ao : &bo;
            bool *other_i = Comm::rank() == 0 ? &bi : &ai;
            *self_i = (zi_i >> i) & 1;
            *self_o = (zi_o >> i) & 1;
            Comm::sexch(self_o, other_i, this->_mpiTime);
            this->_zi += ((ai ^ bi) ^ carry_i) << i;

            // Compute carry_i
            bool generate_i = RsaAndExecutor(ai, bi, false).execute(false)->zi();
            bool propagate_i = ai ^ bi;
            bool tempCarry_i = RsaAndExecutor(propagate_i, carry_i, false).execute(false)->zi();
            bool sum_i = generate_i ^ tempCarry_i;
            bool and_i = RsaAndExecutor(generate_i, tempCarry_i, false).execute(false)->zi();

            carry_i = sum_i ^ and_i;
        }
    }

    return this;
}

template<typename T>
IntExecutor<T> *IntExecutor<T>::convertZiToArithmetic(bool ot) {
    if (Comm::isClient()) {
        return this;
    }
    if (ot) {
        doConvertByOt();
    } else {
        doConvertByFixedRand();
    }
    return this;
}

template<typename T>
void IntExecutor<T>::doConvertByOt() {
    bool isSender = Comm::rank() == 0;
    T xa = 0;
    for (int i = 0; i < this->_l; i++) {
        int xb = (this->_zi >> i) & 1;
        T s0 = 0, s1 = 0;
        int64_t r = 0;
        if (isSender) { // Sender
            r = Math::randInt() & ((1l << this->_l) - 1);
            s0 = (xb << i) - r;
            s1 = ((1 - xb) << i) - r;
        }
        RsaOtExecutor<T> e(0, s0, s1, xb);
        e.logBenchmark(false)->execute(false);
        if (isSender) {
            xa += r;
        } else {
            T s_xb = e.result();
            xa += s_xb;
        }
    }
    this->_zi = xa;
}

template<typename T>
void IntExecutor<T>::doConvertByFixedRand() {
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
std::pair<T, T> IntExecutor<T>::getPair(int idx) {

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

template
class IntExecutor<int8_t>;

template
class IntExecutor<int16_t>;

template
class IntExecutor<int32_t>;

template
class IntExecutor<int64_t>;
