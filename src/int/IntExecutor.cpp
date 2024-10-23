//
// Created by 杜建璋 on 2024/9/6.
//

#include "int/IntExecutor.h"
#include "bit/and/RsaAndExecutor.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Mpi.h"
#include "utils/Math.h"

template<typename T>
IntExecutor<T>::IntExecutor(T z, bool share) {
    if (share) {
        bool detailed = this->_benchmarkLevel == SecureExecutor<T>::BenchmarkLevel::DETAILED;
        // distribute operator
        if (Mpi::isClient()) {
            T z1 = Math::rand64();
            T z0 = z - z1;
            Mpi::send(&z0, 0, this->_mpiTime, detailed);
            Mpi::send(&z1, 1, this->_mpiTime, detailed);
        } else {
            // operator
            Mpi::recv(&this->_zi, Mpi::CLIENT_RANK, this->_mpiTime, detailed);
            Mpi::recv(&this->_zi, Mpi::CLIENT_RANK, this->_mpiTime, detailed);
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
        if (Mpi::isClient()) {
            T x1 = Math::rand64();
            T x0 = x - x1;
            T y1 = Math::rand64();
            T y0 = y - y1;
            Mpi::send(&x0, 0, this->_mpiTime, detailed);
            Mpi::send(&y0, 0, this->_mpiTime, detailed);
            Mpi::send(&x1, 1, this->_mpiTime, detailed);
            Mpi::send(&y1, 1, this->_mpiTime, detailed);
        } else {
            // operator
            Mpi::recv(&_xi, Mpi::CLIENT_RANK, this->_mpiTime, detailed);
            Mpi::recv(&_yi, Mpi::CLIENT_RANK, this->_mpiTime, detailed);
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
    if (Mpi::isServer()) {
        Mpi::send(&this->_zi, Mpi::CLIENT_RANK, this->_mpiTime, detailed);
    } else {
        T z0, z1;
        Mpi::recv(&z0, 0, this->_mpiTime, detailed);
        Mpi::recv(&z1, 1, this->_mpiTime, detailed);
        this->_result = z0 + z1;
    }
    return this;
}

template<typename T>
IntExecutor<T> *IntExecutor<T>::convertZiToBool() {
    if (Mpi::isServer()) {
        // bitwise separate zi
        // zi is xor shared into zi_i and zi_o
        T zi_i = Math::rand64();
        T zi_o = zi_i ^ this->_zi;
        this->_zi = 0;
        bool carry_i = false;

        for (int i = 0; i < this->_l; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = Mpi::rank() == 0 ? &ai : &bi;
            bool *self_o = Mpi::rank() == 0 ? &ao : &bo;
            bool *other_i = Mpi::rank() == 0 ? &bi : &ai;
            *self_i = (zi_i >> i) & 1;
            *self_o = (zi_o >> i) & 1;
            Mpi::sexch(self_o, other_i, this->_mpiTime);
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
IntExecutor<T> *IntExecutor<T>::convertZiToArithmetic() {
    if (Mpi::isServer()) {
        int sender = 0;
        T xa = 0;
        for (int i = 0; i < this->_l; i++) {
            int xb = (this->_zi >> i) & 1;
            T s0 = 0, s1 = 0;
            int64_t r = 0;
            if (Mpi::rank() == sender) { // Sender
                r = Math::rand64() & ((1l << this->_l) - 1);
                s0 = (xb << i) - r;
                s1 = ((1 - xb) << i) - r;
            }
            RsaOtExecutor<T> e(sender, s0, s1, xb);
            e.logBenchmark(false)->execute(false);
            if (Mpi::rank() == sender) {
                xa += r;
            } else {
                T s_xb = e.result();
                xa += s_xb;
            }
        }
        this->_zi = xa;
    }
    return this;
}

template
class IntExecutor<int8_t>;

template
class IntExecutor<int16_t>;

template
class IntExecutor<int32_t>;

template
class IntExecutor<int64_t>;
