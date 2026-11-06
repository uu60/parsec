#include "utils/Crypto.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <random>
#include <vector>

using namespace parsec;

namespace {

// Bit `c` of a 128-bit row: columns 0-63 live in `lo`, 64-127 in `hi`.
bool getBit(const U128 &row, int c) { return c < 64 ? ((row.lo >> c) & 1u) : ((row.hi >> (c - 64)) & 1u); }

void setBit(U128 &row, int c) {
    if (c < 64) {
        row.lo |= uint64_t{1} << c;
    } else {
        row.hi |= uint64_t{1} << (c - 64);
    }
}

std::array<U128, 128> randomMatrix(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::array<U128, 128> m{};
    for (auto &row : m) {
        row.lo = rng();
        row.hi = rng();
    }
    return m;
}

bool equal(const std::array<U128, 128> &a, const std::array<U128, 128> &b) {
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].lo != b[i].lo || a[i].hi != b[i].hi) return false;
    }
    return true;
}

TEST(CryptoTranspose, MovesEachBitToMirroredPosition) {
    for (auto [r, c] : std::array<std::pair<int, int>, 5>{{{0, 0}, {0, 127}, {5, 70}, {100, 3}, {127, 64}}}) {
        std::array<U128, 128> m{};
        setBit(m[r], c);
        Crypto::transpose128x128_inplace(m.data());
        for (int i = 0; i < 128; ++i) {
            for (int j = 0; j < 128; ++j) {
                ASSERT_EQ(getBit(m[i], j), i == c && j == r) << "bit (" << r << "," << c << ") -> (" << i << "," << j
                                                             << ")";
            }
        }
    }
}

TEST(CryptoTranspose, MatchesReferenceAndIsAnInvolution) {
    const auto original = randomMatrix(1);
    auto m = original;
    Crypto::transpose128x128_inplace(m.data());
    for (int i = 0; i < 128; ++i) {
        for (int j = 0; j < 128; ++j) {
            ASSERT_EQ(getBit(m[i], j), getBit(original[j], i));
        }
    }
    Crypto::transpose128x128_inplace(m.data());
    EXPECT_TRUE(equal(m, original));
}

TEST(CryptoTranspose, ColumnExtractionMatchesFullTranspose) {
    const auto original = randomMatrix(2);
    auto transposed = original;
    Crypto::transpose128x128_inplace(transposed.data());

    const U128 col0 = Crypto::extract_col0_from_rows(original.data());
    const U128 col64 = Crypto::extract_col64_from_rows(original.data());
    EXPECT_EQ(col0.lo, transposed[0].lo);
    EXPECT_EQ(col0.hi, transposed[0].hi);
    EXPECT_EQ(col64.lo, transposed[64].lo);
    EXPECT_EQ(col64.hi, transposed[64].hi);
}

TEST(CryptoPrg, BatchIsDeterministicAndSeedDependent) {
    constexpr size_t kSeeds = 4;
    constexpr size_t kBlocks = 8;
    const uint64_t seeds[kSeeds] = {1, 2, 3, 1};
    std::vector<U128> first(kSeeds * kBlocks), second(kSeeds * kBlocks);
    Crypto::batchPrgGenerate(seeds, kSeeds, first.data(), kBlocks);
    Crypto::batchPrgGenerate(seeds, kSeeds, second.data(), kBlocks);

    for (size_t i = 0; i < first.size(); ++i) {
        EXPECT_EQ(first[i].lo, second[i].lo);
        EXPECT_EQ(first[i].hi, second[i].hi);
    }
    // Seed 0 and seed 3 are equal, seeds 0 and 1 differ.
    for (size_t b = 0; b < kBlocks; ++b) {
        EXPECT_EQ(first[0 * kBlocks + b].lo, first[3 * kBlocks + b].lo);
        EXPECT_EQ(first[0 * kBlocks + b].hi, first[3 * kBlocks + b].hi);
    }
    bool differs = false;
    for (size_t b = 0; b < kBlocks; ++b) {
        differs |= first[0 * kBlocks + b].lo != first[1 * kBlocks + b].lo;
    }
    EXPECT_TRUE(differs);
}

// AesCtrPrg is a separate construction from batchPrgGenerate (per-seed AES-CTR vs fixed-key AES), so only its own
// properties are checked here.
TEST(CryptoPrg, AesCtrIsDeterministicAndPrefixStable) {
    std::vector<U128> a(8), b(8), prefix(3), other(8);
    Crypto::AesCtrPrg(0x1234).generateBlocks(a.data(), a.size());
    Crypto::AesCtrPrg(0x1234).generateBlocks(b.data(), b.size());
    Crypto::AesCtrPrg(0x1234).generateBlocks(prefix.data(), prefix.size());
    Crypto::AesCtrPrg(0x1235).generateBlocks(other.data(), other.size());

    bool differs = false;
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].lo, b[i].lo);
        EXPECT_EQ(a[i].hi, b[i].hi);
        differs |= a[i].lo != other[i].lo || a[i].hi != other[i].hi;
    }
    for (size_t i = 0; i < prefix.size(); ++i) {
        EXPECT_EQ(prefix[i].lo, a[i].lo);
        EXPECT_EQ(prefix[i].hi, a[i].hi);
    }
    EXPECT_TRUE(differs);
}

TEST(CryptoHash, DeterministicAndIndexDependent) {
    const U128 v{0x0123456789abcdefULL, 0xfedcba9876543210ULL};
    EXPECT_EQ(Crypto::hash64(3, v), Crypto::hash64(3, v));
    EXPECT_NE(Crypto::hash64(3, v), Crypto::hash64(4, v));
    EXPECT_EQ(Crypto::hash64Fast(3, v), Crypto::hash64Fast(3, v));
    EXPECT_NE(Crypto::hash64Fast(3, v), Crypto::hash64Fast(4, v));
    EXPECT_EQ(Crypto::hash64(9, uint64_t{42}), Crypto::hash64(9, uint64_t{42}));
}

} // namespace
