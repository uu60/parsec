//
// Created by 杜建璋 on 2024/9/6.
//

#include "int/IntArithExecutor.h"
#include "bit/and/BitAndExecutor.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"

template<typename T>
IntArithExecutor<T>::IntArithExecutor(T z, bool share) {
    if (share) {
        // distribute operator
        if (Comm::isClient()) {
            T z1 = Math::randInt();
            T z0 = z - z1;
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

template<typename T>
IntArithExecutor<T>::IntArithExecutor(T x, T y, bool share) {
    if (share) {
        // distribute operator
        if (Comm::isClient()) {
            T x1 = Math::randInt();
            T x0 = x - x1;
            T y1 = Math::randInt();
            T y0 = y - y1;
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
IntArithExecutor<T> *IntArithExecutor<T>::execute(bool reconstruct) {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
std::string IntArithExecutor<T>::tag() const {
    throw std::runtime_error("This method cannot be called!");
}

template<typename T>
IntArithExecutor<T> *IntArithExecutor<T>::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&this->_zi, Comm::CLIENT_RANK);
    } else {
        T z0, z1;
        Comm::recv(&z0, 0);
        Comm::recv(&z1, 1);
        this->_result = z0 + z1;
    }
    return this;
}

template<typename T>
T IntArithExecutor<T>::convertZiToBool() {
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
            Comm::sexch(self_o, other_i);
            this->_zi += ((ai ^ bi) ^ carry_i) << i;

            // Compute carry_i
            bool generate_i = BitAndExecutor(ai, bi, false).execute(false)->_zi;
            bool propagate_i = ai ^ bi;
            bool tempCarry_i = BitAndExecutor(propagate_i, carry_i, false).execute(false)->_zi;
            bool sum_i = generate_i ^ tempCarry_i;
            bool and_i = BitAndExecutor(generate_i, tempCarry_i, false).execute(false)->_zi;

            carry_i = sum_i ^ and_i;
        }
    }

    return this->_zi;
}

template
class IntArithExecutor<int8_t>;

template
class IntArithExecutor<int16_t>;

template
class IntArithExecutor<int32_t>;

template
class IntArithExecutor<int64_t>;
