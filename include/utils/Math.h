//
// Created by 杜建璋 on 2024/7/6.
//

#ifndef MPC_PACKAGE_MATH_H
#define MPC_PACKAGE_MATH_H

#include <string>
#include <bitset>
#include <vector>

class Math {
public:
    // generate a random
    static int64_t randInt();

    static int64_t randInt(int64_t lowest, int64_t highest);

    static int64_t pow(int64_t base, int64_t exponent);

    static std::string randString(int bytes);

    // '1' + 1 = 49 + 1 = 50 = '2'
    static std::string add(const std::string &add0, int64_t add1);

    static std::string add(const std::string &add0, const std::string &add1);

    static std::string minus(const std::string &add0, const std::string &add1);

    // dividend % divisor
    // divisor should be positive
    static int64_t ring(int64_t num, int width);

    static bool getBit(int64_t v, int i);

    static int64_t changeBit(int64_t v, int i, bool b);

    template<int width>
    static std::string toBinString(int64_t v) {
        return std::bitset<width>(v).to_string();
    };
};


#endif //MPC_PACKAGE_MATH_H
