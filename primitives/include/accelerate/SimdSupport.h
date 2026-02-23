
#ifndef SIMDSUPPORT_H
#define SIMDSUPPORT_H
#include <iostream>
#include <vector>
#include <cstdint>
#include "utils/Crypto.h"  // For U128

class SimdSupport {
public:
    static std::vector<int64_t> xorV(const std::vector<int64_t> &arr0, const std::vector<int64_t> &arr1);

    static std::vector<int64_t> andV(const std::vector<int64_t> &arr0, const std::vector<int64_t> &arr1);

    static std::vector<int64_t> andVC(const std::vector<int64_t> &arr, int64_t constant);

    static std::vector<int64_t> orV(const std::vector<int64_t> &arr0, const std::vector<int64_t> &arr1);

    static std::vector<int64_t> xorVC(const std::vector<int64_t> &arr, int64_t constant);

    static std::vector<int64_t> xor2VC(const std::vector<int64_t> &xis,
                                       const std::vector<int64_t> &yis,
                                       int64_t a, int64_t b);

    static std::vector<int64_t> xor3(const int64_t *a, const int64_t *b,
                                     const int64_t *c,
                                     int num);

    static std::vector<int64_t> xor3Concat(const int64_t *commonA,
                                           const int64_t *arrB,
                                           const int64_t *arrD,
                                           const int64_t *commonC,
                                           int num);

    static std::vector<int64_t> computeZ(const std::vector<int64_t> &efs, int64_t a, int64_t b, int64_t c);

    static std::vector<int64_t> computeDiag(const std::vector<int64_t> &_yis, const std::vector<int64_t> &x_xor_y);

    // ============== U128 SIMD Operations for IKNP ==============

    // XOR array of U128 with constant U128: out[i] = arr[i] XOR constant
    static void xorU128ArrayWithConstant(U128* out, const U128* arr, const U128& constant, size_t count);

    // XOR two U128 arrays: out[i] = arr0[i] XOR arr1[i]
    static void xorU128Arrays(U128* out, const U128* arr0, const U128* arr1, size_t count);

    // XOR three U128 arrays: out[i] = arr0[i] XOR arr1[i] XOR arr2[i]
    static void xor3U128Arrays(U128* out, const U128* arr0, const U128* arr1, const U128* arr2, size_t count);

    // Conditional XOR: if mask[i] is true, out[i] ^= src[i]
    static void conditionalXorU128(U128* out, const U128* src, const bool* mask, size_t count);

    // Select bits from y0 or y1 based on choice: result = (y0 & ~choice) | (y1 & choice)
    static void selectBits(int64_t* result, const int64_t* y0, const int64_t* y1,
                          const int64_t* choice, const int64_t* hashValues, size_t count);
};


#endif
