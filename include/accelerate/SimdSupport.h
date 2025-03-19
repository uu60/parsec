//
// Created by 杜建璋 on 2025/3/11.
//

#ifndef SIMDSUPPORT_H
#define SIMDSUPPORT_H
#include <iostream>
#include <vector>
#include <cstdint>

class SimdSupport {
public:
    // xor 2 vectors
    static std::vector<int64_t> xorV(const std::vector<int64_t> &arr0, const std::vector<int64_t> &arr1);

    // and 2 vectors
    static std::vector<int64_t> andV(const std::vector<int64_t> &arr0, const std::vector<int64_t> &arr1);

    // and a vector and a constant value
    static std::vector<int64_t> andVC(const std::vector<int64_t> &arr, int64_t constant);

    // or 2 vectors
    static std::vector<int64_t> orV(const std::vector<int64_t> &arr0, const std::vector<int64_t> &arr1);

    // xor a vector and a constant value
    static std::vector<int64_t> xorVC(const std::vector<int64_t> &arr, int64_t constant);

    // xor 2 vectors and 2 constants respectively and combine the results
    static std::vector<int64_t> xor2VC(const std::vector<int64_t> &xis,
                                                 const std::vector<int64_t> &yis,
                                                 int64_t a, int64_t b);

    // xor 3 arrays with specific element number
    static std::vector<int64_t> xor3(const int64_t *a, const int64_t *b,
                                     const int64_t *c,
                                     int num);

    // compute z
    static std::vector<int64_t> computeZ(const std::vector<int64_t> &efs, int64_t a, int64_t b, int64_t c);
};


#endif //SIMDSUPPORT_H
