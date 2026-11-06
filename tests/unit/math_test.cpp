#include "utils/Math.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace parsec;

namespace {

constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

TEST(MathRing, ReducesToWidth) {
    EXPECT_EQ(Math::ring(-1, 1), 1);
    EXPECT_EQ(Math::ring(-1, 8), 255);
    EXPECT_EQ(Math::ring(256 + 7, 8), 7);
    EXPECT_EQ(Math::ring(-1, 64), -1);
    EXPECT_EQ(Math::ring(kMin, 64), kMin);
}

TEST(MathRing, RejectsInvalidWidth) {
    EXPECT_THROW(Math::ring(1, 0), std::invalid_argument);
    EXPECT_THROW(Math::ring(1, 65), std::invalid_argument);
}

TEST(MathRing, ArithmeticWrapsAt64Bits) {
    EXPECT_EQ(Math::ringAdd(kMax, 1, 64), kMin);
    EXPECT_EQ(Math::ringSubtract(kMin, 1, 64), kMax);
    EXPECT_EQ(Math::ringMultiply(kMax, 2, 64), -2);
}

TEST(MathRing, ArithmeticMatchesModularReference) {
    std::mt19937_64 rng(42);
    for (int width : {1, 7, 8, 16, 31, 32, 63}) {
        const uint64_t mask = (uint64_t{1} << width) - 1;
        for (int i = 0; i < 200; ++i) {
            const auto a = static_cast<int64_t>(rng());
            const auto b = static_cast<int64_t>(rng());
            const uint64_t ua = static_cast<uint64_t>(a) & mask;
            const uint64_t ub = static_cast<uint64_t>(b) & mask;
            EXPECT_EQ(static_cast<uint64_t>(Math::ringAdd(a, b, width)), (ua + ub) & mask) << "width " << width;
            EXPECT_EQ(static_cast<uint64_t>(Math::ringSubtract(a, b, width)), (ua - ub) & mask) << "width " << width;
            EXPECT_EQ(static_cast<uint64_t>(Math::ringMultiply(a, b, width)), (ua * ub) & mask) << "width " << width;
        }
    }
}

TEST(MathBits, GetAndChangeBit) {
    EXPECT_EQ(Math::changeBit(0, 63, true), kMin);
    EXPECT_EQ(Math::changeBit(-1, 63, false), kMax);
    EXPECT_TRUE(Math::getBit(kMin, 63));
    EXPECT_FALSE(Math::getBit(kMax, 63));
    for (int i = 0; i < 64; ++i) {
        const int64_t v = Math::changeBit(0, i, true);
        EXPECT_TRUE(Math::getBit(v, i));
        EXPECT_EQ(Math::changeBit(v, i, false), 0);
    }
}

TEST(MathBits, ToBinString) {
    EXPECT_EQ(Math::toBinString<8>(5), "00000101");
    EXPECT_EQ(Math::toBinString<4>(-1), "1111");
}

TEST(MathPow, SmallPowers) {
    EXPECT_EQ(Math::pow(2, 0), 1);
    EXPECT_EQ(Math::pow(2, 10), 1024);
    EXPECT_EQ(Math::pow(-3, 3), -27);
    EXPECT_EQ(Math::pow(7, 1), 7);
}

// Big-number helpers operate on big-endian binary strings.
TEST(MathBigNum, AddAndMinusBinaryStrings) {
    EXPECT_EQ(Math::add(std::string("\x01", 1), std::string("\x02", 1)), std::string("\x03", 1));
    EXPECT_EQ(Math::add(std::string("\xff", 1), std::string("\x01", 1)), std::string("\x01\x00", 2));

    std::mt19937_64 rng(7);
    for (int i = 0; i < 50; ++i) {
        std::string a(16, '\0'), b(16, '\0');
        for (auto& ch : a)
            ch = static_cast<char>(rng());
        for (auto& ch : b)
            ch = static_cast<char>(rng());
        a[0] = static_cast<char>(a[0] | 0x01); // keep a non-zero leading byte so lengths round-trip
        EXPECT_EQ(Math::minus(Math::add(a, b), b), a);
    }
}

} // namespace

namespace {

// Each of the 64 bits should be set in about half of the samples. With 20000 samples the standard deviation of a
// bit count is about 71, so [9000, 11000] is a > 14-sigma band: it catches stuck bits (such as an always-zero top bit)
// without being flaky.
TEST(MathRandom, RandBitsCoversAllBitsUniformly) {
    constexpr int kSamples = 20000;
    std::array<int, 64> ones{};
    for (int s = 0; s < kSamples; ++s) {
        const auto v = static_cast<uint64_t>(Math::randBits());
        for (int b = 0; b < 64; ++b) {
            ones[b] += static_cast<int>((v >> b) & 1u);
        }
    }
    for (int b = 0; b < 64; ++b) {
        EXPECT_GT(ones[b], 9000) << "bit " << b;
        EXPECT_LT(ones[b], 11000) << "bit " << b;
    }
}

TEST(MathRandom, RandIntIsNonNegativeAndUsesBit62) {
    bool sawBit62 = false;
    for (int i = 0; i < 2000; ++i) {
        const int64_t v = Math::randInt();
        ASSERT_GE(v, 0);
        sawBit62 |= ((v >> 62) & 1) != 0;
    }
    EXPECT_TRUE(sawBit62);
}

TEST(MathRandom, RandIntRangeStaysInBoundsAndHitsEveryValue) {
    std::array<int, 7> counts{};
    for (int i = 0; i < 7000; ++i) {
        const int64_t v = Math::randInt(-3, 3);
        ASSERT_GE(v, -3);
        ASSERT_LE(v, 3);
        ++counts[static_cast<size_t>(v + 3)];
    }
    for (int c : counts) {
        EXPECT_GT(c, 700); // expected 1000 each
    }
    EXPECT_EQ(Math::randInt(5, 5), 5);
    EXPECT_THROW(Math::randInt(2, 1), std::invalid_argument);
    // Full 64-bit range must not overflow.
    bool sawNegative = false, sawPositive = false;
    for (int i = 0; i < 200; ++i) {
        const int64_t v = Math::randInt(kMin, kMax);
        sawNegative |= v < 0;
        sawPositive |= v > 0;
    }
    EXPECT_TRUE(sawNegative && sawPositive);
}

TEST(MathRandom, RandStringHasRequestedLengthAndVaries) {
    EXPECT_TRUE(Math::randString(0).empty());
    const std::string a = Math::randString(32);
    const std::string b = Math::randString(32);
    EXPECT_EQ(a.size(), 32u);
    EXPECT_NE(a, b);
    EXPECT_THROW(Math::randString(-1), std::invalid_argument);
}

TEST(MathRandom, ThreadsDrawIndependentValues) {
    std::array<int64_t, 4> first{};
    std::vector<std::thread> threads;
    for (size_t t = 0; t < first.size(); ++t) {
        threads.emplace_back([&first, t] { first[t] = Math::randBits(); });
    }
    for (auto& th : threads)
        th.join();
    for (size_t i = 0; i < first.size(); ++i) {
        for (size_t j = i + 1; j < first.size(); ++j) {
            EXPECT_NE(first[i], first[j]);
        }
    }
}

} // namespace

namespace {

TEST(MathKdf, SixteenByteKdfExtendsEightByteKdf) {
    for (const std::string input : {std::string(), std::string("abc"), std::string(300, 'x')}) {
        const auto wide = Math::kdfSha256To16Bytes(input);
        EXPECT_EQ(wide[0], Math::kdfSha256To8Bytes(input));
        EXPECT_NE(wide[0], wide[1]);
    }
}

} // namespace
