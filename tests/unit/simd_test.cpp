#include "accelerate/SimdSupport.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <vector>

using namespace parsec;

namespace {

std::vector<int64_t> randomVector(size_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<int64_t> v(n);
    for (auto &x : v) x = static_cast<int64_t>(rng());
    return v;
}

// Odd lengths exercise both the vectorized body and the scalar tail.
class SimdVectorOps : public ::testing::TestWithParam<size_t> {};

TEST_P(SimdVectorOps, MatchScalarReference) {
    const size_t n = GetParam();
    const auto a = randomVector(n, 1);
    const auto b = randomVector(n, 2);
    const int64_t k = 0x5a5a5a5a5a5a5a5aLL;

    const auto x = SimdSupport::xorV(a, b);
    const auto y = SimdSupport::andV(a, b);
    const auto o = SimdSupport::orV(a, b);
    const auto xc = SimdSupport::xorVC(a, k);
    const auto ac = SimdSupport::andVC(a, k);
    ASSERT_EQ(x.size(), n);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(x[i], a[i] ^ b[i]);
        EXPECT_EQ(y[i], a[i] & b[i]);
        EXPECT_EQ(o[i], a[i] | b[i]);
        EXPECT_EQ(xc[i], a[i] ^ k);
        EXPECT_EQ(ac[i], a[i] & k);
    }
}

TEST_P(SimdVectorOps, U128XorMatchesScalarReference) {
    const size_t n = GetParam();
    std::mt19937_64 rng(3);
    std::vector<U128> a(n), b(n), c(n), out(n);
    for (size_t i = 0; i < n; ++i) {
        a[i] = {rng(), rng()};
        b[i] = {rng(), rng()};
        c[i] = {rng(), rng()};
    }
    SimdSupport::xorU128Arrays(out.data(), a.data(), b.data(), n);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(out[i].lo, a[i].lo ^ b[i].lo);
        EXPECT_EQ(out[i].hi, a[i].hi ^ b[i].hi);
    }
    SimdSupport::xor3U128Arrays(out.data(), a.data(), b.data(), c.data(), n);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(out[i].lo, a[i].lo ^ b[i].lo ^ c[i].lo);
        EXPECT_EQ(out[i].hi, a[i].hi ^ b[i].hi ^ c[i].hi);
    }
}

INSTANTIATE_TEST_SUITE_P(Lengths, SimdVectorOps, ::testing::Values(0, 1, 3, 4, 17, 1000));

} // namespace
