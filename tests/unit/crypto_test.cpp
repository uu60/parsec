#include "parsec/utils/Crypto.h"

#include <gtest/gtest.h>
#include <openssl/evp.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

using namespace parsec;

namespace {

// Bit `c` of a 128-bit row: columns 0-63 live in `lo`, 64-127 in `hi`.
bool getBit(const U128& row, int c) { return c < 64 ? ((row.lo >> c) & 1u) : ((row.hi >> (c - 64)) & 1u); }

void setBit(U128& row, int c) {
    if (c < 64) {
        row.lo |= uint64_t{1} << c;
    } else {
        row.hi |= uint64_t{1} << (c - 64);
    }
}

std::array<U128, 128> randomMatrix(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::array<U128, 128> m{};
    for (auto& row : m) {
        row.lo = rng();
        row.hi = rng();
    }
    return m;
}

bool equal(const std::array<U128, 128>& a, const std::array<U128, 128>& b) {
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].lo != b[i].lo || a[i].hi != b[i].hi)
            return false;
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
                ASSERT_EQ(getBit(m[i], j), i == c && j == r)
                    << "bit (" << r << "," << c << ") -> (" << i << "," << j << ")";
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

std::vector<U128> randomSeeds(size_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<U128> seeds(n);
    for (auto& s : seeds) {
        s = {rng(), rng()};
    }
    return seeds;
}

// Reference: block b of stream t for seed k is AES-128_k(le64(nonce.lo ^ b) || le64(nonce.hi ^ t)).
U128 referenceBlock(const U128& seed, const U128& nonce, uint64_t stream, uint64_t block) {
    unsigned char key[16], in[16], out[32];
    for (int i = 0; i < 8; ++i) {
        key[i] = static_cast<unsigned char>(seed.lo >> (8 * i));
        key[8 + i] = static_cast<unsigned char>(seed.hi >> (8 * i));
        in[i] = static_cast<unsigned char>((nonce.lo ^ block) >> (8 * i));
        in[8 + i] = static_cast<unsigned char>((nonce.hi ^ stream) >> (8 * i));
    }
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    int len = 0;
    EVP_EncryptInit_ex(ctx, EVP_aes_128_ecb(), nullptr, key, nullptr);
    EVP_CIPHER_CTX_set_padding(ctx, 0);
    EVP_EncryptUpdate(ctx, out, &len, in, 16);
    EVP_CIPHER_CTX_free(ctx);
    U128 r{};
    std::memcpy(&r, out, 16);
    return r;
}

TEST(CryptoSeededPrg, MatchesAesReference) {
    const auto seeds = randomSeeds(4, 11);
    const U128 nonce{0x0123456789abcdefULL, 0x0fedcba987654321ULL};
    constexpr size_t kBlocks = 5;
    Crypto::SeededPrg prg(seeds.data(), seeds.size());
    std::vector<U128> out(seeds.size() * kBlocks);
    prg.generate(nonce, 7, out.data(), kBlocks);
    for (size_t i = 0; i < seeds.size(); ++i) {
        for (size_t b = 0; b < kBlocks; ++b) {
            const U128 expected = referenceBlock(seeds[i], nonce, 7, b);
            EXPECT_EQ(out[i * kBlocks + b].lo, expected.lo) << "seed " << i << " block " << b;
            EXPECT_EQ(out[i * kBlocks + b].hi, expected.hi) << "seed " << i << " block " << b;
        }
    }
}

TEST(CryptoSeededPrg, OutputDependsOnSeedNonceAndStream) {
    const auto seeds = randomSeeds(2, 12);
    Crypto::SeededPrg prg(seeds.data(), seeds.size());
    const U128 nonceA{1, 2}, nonceB{1, 3};
    std::vector<U128> a(2), again(2), otherNonce(2), otherStream(2);
    prg.generate(nonceA, 0, a.data(), 1);
    prg.generate(nonceA, 0, again.data(), 1);
    prg.generate(nonceB, 0, otherNonce.data(), 1);
    prg.generate(nonceA, 1, otherStream.data(), 1);
    for (size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(a[i].lo, again[i].lo);
        EXPECT_EQ(a[i].hi, again[i].hi);
        EXPECT_FALSE(a[i].lo == otherNonce[i].lo && a[i].hi == otherNonce[i].hi);
        EXPECT_FALSE(a[i].lo == otherStream[i].lo && a[i].hi == otherStream[i].hi);
    }
    // Seeds that differ only in the high 64 bits must give different streams (full 128-bit keys).
    const U128 pair[2] = {{5, 0}, {5, 1}};
    Crypto::SeededPrg hiOnly(pair, 2);
    std::vector<U128> out(2);
    hiOnly.generate(nonceA, 0, out.data(), 1);
    EXPECT_FALSE(out[0].lo == out[1].lo && out[0].hi == out[1].hi);
}

TEST(CryptoSeededPrg, BlocksWithinAStreamAreDistinct) {
    const auto seeds = randomSeeds(1, 13);
    Crypto::SeededPrg prg(seeds.data(), 1);
    std::vector<U128> out(64);
    prg.generate({9, 9}, 3, out.data(), out.size());
    for (size_t i = 0; i < out.size(); ++i) {
        for (size_t j = i + 1; j < out.size(); ++j) {
            EXPECT_FALSE(out[i].lo == out[j].lo && out[i].hi == out[j].hi);
        }
    }
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
