//
// Created by 杜建璋 on 2024/7/6.
//

#include "utils/Math.h"
#include <random>
#include <string>
#include <limits>
#include <iomanip>
#include <openssl/bn.h>
#include <openssl/evp.h>

BIGNUM *bignum(const std::string &str, const bool positive) {
    std::vector<uint8_t> binaryData = std::vector<uint8_t>(str.begin(), str.end());
    BIGNUM *bn = BN_new();
    BN_bin2bn(binaryData.data(), binaryData.size(), bn);
    if (!positive) {
        BN_set_negative(bn, 1);
    }
    return bn;
}

BIGNUM *bignum(const std::string &str) {
    return bignum(str, true);
}

std::string string(BIGNUM *bn) {
    int num_bytes = BN_num_bytes(bn);
    std::vector<unsigned char> bin(num_bytes);
    BN_bn2bin(bn, bin.data());

    std::string binary_str;
    for (unsigned char byte: bin) {
        for (int i = 7; i >= 0; --i) {
            binary_str.push_back((byte & (1 << i)) ? '1' : '0');
        }
    }
    std::string result;
    for (size_t i = 0; i < binary_str.size(); i += 8) {
        std::string byte_str = binary_str.substr(i, 8);
        char byte = static_cast<char>(std::stoi(byte_str, nullptr, 2));
        result.push_back(byte);
    }
    return result;
}

BIGNUM *add_bignum_with_int(BIGNUM *add0, int64_t add1) {
    BIGNUM *bn_add1 = BN_new();
    if (add1 >= 0) {
        BN_set_word(bn_add1, add1);
    } else {
        BN_set_word(bn_add1, abs(add1));
        BN_set_negative(bn_add1, 1);
    }
    BIGNUM *result = BN_new();
    BN_add(result, add0, bn_add1);
    BN_free(bn_add1);
    return result;
}

std::string add_or_minus(const std::string &add0, const std::string &add1, bool minus) {
    BIGNUM *add0N = bignum(add0);
    BIGNUM *add1N = bignum(add1, !minus);
    BIGNUM *result = BN_new();
    BN_add(result, add0N, add1N);
    std::string resultStr = string(result);
    BN_free(add0N);
    BN_free(add1N);
    BN_free(result);
    return resultStr;
}

int64_t Math::randInt() {
    return randInt(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
}

int64_t Math::randInt(int64_t lowest, int64_t highest) {
    // random engine
    std::random_device rd;
    std::mt19937 generator(rd());

    // distribution in integer range
    std::uniform_int_distribution<int64_t> distribution(lowest, highest);

    // generation
    return distribution(generator);
}

std::string Math::add(const std::string &add0, int64_t add1) {
    BIGNUM *add0N = bignum(add0);
    BIGNUM *sum = add_bignum_with_int(add0N, add1);
    std::string result = string(sum);
    BN_free(add0N);
    BN_free(sum);
    return result;
}

std::string Math::randString(int bytes) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int8_t> dis(-128, 127);

    std::vector<int8_t> temp;
    temp.reserve(bytes);
    for (size_t i = 0; i < bytes; ++i) {
        temp.push_back(dis(gen));
    }

    return std::string(temp.begin(), temp.end());
}

std::string Math::add(const std::string &add0, const std::string &add1) {
    return add_or_minus(add0, add1, false);
}

std::string Math::minus(const std::string &add0, const std::string &add1) {
    return add_or_minus(add0, add1, true);
}

int64_t Math::ring(int64_t num, int l) {
    if (l == 64) {
        return num;
    }
    return num & ((1LL << l) - 1);
}

bool Math::getBit(int64_t v, int i) {
    return (v >> i) & 1;
}

int64_t Math::changeBit(int64_t v, int i, bool b) {
    int64_t mask = ~(1LL << i);
    return (v & mask) | ((b ? 1ll : 0ll) << i);
}

int64_t Math::pow(int64_t base, int64_t exponent) {
    int64_t result = 1;
    while (exponent > 0) {
        if (exponent % 2 == 1) {
            result *= base;
        }
        base *= base;
        exponent /= 2;
    }
    return result;
}
