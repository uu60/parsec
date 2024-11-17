//
// Created by 杜建璋 on 2024/11/7.
//

#include "../../include/compute/BoolExecutor.h"
#include "../../include/utils/Comm.h"
#include "../../include/utils/Math.h"
#include "../../include/ot/RsaOtExecutor.h"
#if __has_include("compute/fixed_ab_pairs_0.h")

#include "../../include/compute/fixed_ab_pairs_0.h"

#endif

#if __has_include("compute/fixed_ab_pairs_1.h")

#include "../../include/compute/fixed_ab_pairs_1.h"

#endif

BoolExecutor::BoolExecutor(int64_t z, int l, bool local) : SecureExecutor(l) {
    if (local) {
        _zi = ring(z);
    } else {
        // distribute operator
        if (Comm::isClient()) {
            int64_t z1 = ring(Math::randInt());
            int64_t z0 = ring(z ^ z1);
            Comm::send(&z0, 0);
            Comm::send(&z1, 1);
        } else {
            // operator
            Comm::recv(&_zi, Comm::CLIENT_RANK);
        }
    }
}

BoolExecutor::BoolExecutor(int64_t x, int64_t y, int l, bool local) : SecureExecutor(l) {
    if (local) {
        _xi = ring(x);
        _yi = ring(y);
    } else {
        // distribute operator
        if (Comm::isClient()) {
            int64_t x1 = ring(Math::randInt());
            int64_t x0 = ring(x ^ x1);
            int64_t y1 = ring(Math::randInt());
            int64_t y0 = ring(y ^ y1);
            Comm::send(&x0, 0);
            Comm::send(&y0, 0);
            Comm::send(&x1, 1);
            Comm::send(&y1, 1);
        } else {
            // operator
            Comm::recv(&_xi, Comm::CLIENT_RANK);
            Comm::recv(&_yi, Comm::CLIENT_RANK);
        }
    }
}

int64_t BoolExecutor::arithZi() const {
    int64_t xa = 0;
    if (Comm::isServer()) {
        bool isSender = Comm::rank() == 0;
        for (int i = 0; i < _l; i++) {
            int xb = static_cast<int>((_zi >> i) & 1);
            int64_t s0 = 0, s1 = 0;
            int64_t r = 0;
            if (isSender) {
                // Sender
                r = ring(Math::randInt());
                s0 = ring((xb << i) - r);
                s1 = ring(((1 - xb) << i) - r);
            }
            RsaOtExecutor e(0, s0, s1, _l, xb);
            e.execute();
            if (isSender) {
                xa = ring(xa + r);
            } else {
                int64_t s_xb = e._result;
                xa = ring(xa + s_xb);
            }
        }
    }
    return xa;
}

// void IntBoolExecutor::doPregeneratedConvert() {
//     int64_t res = 0;
//     for (compute i = 0; i < _l; i++) {
//         int64_t idx = 0;
//         if (Comm::rank() == 0) {
//             idx = Math::randInt(0, 99);
//             Comm::ssend(&idx);
//         } else {
//             Comm::srecv(&idx);
//         }
//         std::pair<int64_t, int64_t> r = getPair(idx);
//         int64_t ri_b = r.first;
//         int64_t ri_a = r.second;
//
//         // Compute
//         int64_t zi_b = ((_zi >> i) & 1) ^ ri_b;
//         int64_t zo_b;
//
//         // Decrypt
//         Comm::sexch(&zi_b, &zo_b);
//         int64_t z = zo_b ^ zi_b;
//         int64_t zi_a;
//
//         // Compute
//         res += (ri_a + z * Comm::rank() - 2 * ri_a * z) << i;
//     }
//     _zi = res;
// }
//
// std::pair<T, T> IntBoolExecutor::getPair(compute idx) {
//     const std::array<T, 100> *booleans;
//     const std::array<T, 100> *ariths;
//     if constexpr (std::is_same_v<T, int8_t>) {
//         booleans = &R_BOOL_8;
//         ariths = &R_ARITH_8;
//     } else if constexpr (std::is_same_v<T, int16_t>) {
//         booleans = &R_BOOL_16;
//         ariths = &R_ARITH_16;
//     } else if constexpr (std::is_same_v<T, int32_t>) {
//         booleans = &R_BOOL_32;
//         ariths = &R_ARITH_32;
//     } else {
//         booleans = &R_BOOL_64;
//         ariths = &R_ARITH_64;
//     }
//     return {(*booleans)[idx], (*ariths)[idx]};
// }

BoolExecutor *BoolExecutor::reconstruct() {
    if (Comm::isServer()) {
        Comm::send(&_zi, Comm::CLIENT_RANK);
    } else {
        int64_t z0, z1;
        Comm::recv(&z0, 0);
        Comm::recv(&z1, 1);
        _result = z0 ^ z1;
    }
    return this;
}

BoolExecutor *BoolExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string BoolExecutor::tag() const {
    throw std::runtime_error("This method cannot be called!");
}
