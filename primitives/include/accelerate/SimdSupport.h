
#ifndef SIMDSUPPORT_H
#define SIMDSUPPORT_H
#include <iostream>
#include <vector>
#include <cstdint>

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
};


#endif
