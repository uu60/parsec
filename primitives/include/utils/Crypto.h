//
// #ifndef MPC_PACKAGE_CRYPTO_H
// #define MPC_PACKAGE_CRYPTO_H
// #include <iostream>
// #include <unordered_map>
//
// #include <string>
// class Crypto {
// public:
//     static std::unordered_map<int, std::string> _selfPubs;
//     static std::unordered_map<int, std::string> _selfPris;
//     static std::unordered_map<int, std::string> _otherPubs;
//
//     static bool generateRsaKeys(int bits);
//
//     static std::string rsaEncrypt(const std::string &data, const std::string &publicKey);
//
//     static std::string rsaDecrypt(const std::string &encryptedData, const std::string &privateKey);
// };
//
//
// #endif

#ifndef MPC_PACKAGE_CRYPTO_H
#define MPC_PACKAGE_CRYPTO_H
#include <iostream>
#include <unordered_map>
#include <array>
#include <cstdint>
#include <vector>
#include <string>

// 128-bit data structure for cryptographic operations
struct U128 {
    uint64_t lo;
    uint64_t hi;
};

static_assert(sizeof(U128) == 16, "U128 must be 16 bytes");
static_assert(alignof(U128) >= alignof(uint64_t), "U128 alignment unexpected");

class Crypto {
public:
    static std::unordered_map<int, std::string> _selfPubs;
    static std::unordered_map<int, std::string> _selfPris;
    static std::unordered_map<int, std::string> _otherPubs;

    // RSA encryption methods
    static bool generateRsaKeys(int bits);
    static std::string rsaEncrypt(const std::string &data, const std::string &publicKey);
    static std::string rsaDecrypt(const std::string &encryptedData, const std::string &privateKey);

    // AES-CTR PRG (Pseudo-Random Generator)
    class AesCtrPrg {
    public:
        explicit AesCtrPrg(uint64_t seed);
        ~AesCtrPrg();

        void generateBlocks(U128 *out, size_t blocks) const;

    private:
        std::array<unsigned char, 16> _key{};
        std::array<unsigned char, 16> _iv{};
    };

    // Batch PRG generation for multiple seeds - much faster than creating individual AesCtrPrg objects
    // Generates PRG output for 128 seeds at once, each producing 'blocksPerSeed' U128 blocks
    // Output layout: out[seed * blocksPerSeed + block]
    static void batchPrgGenerate(const uint64_t* seeds, size_t numSeeds,
                                  U128* out, size_t blocksPerSeed);

    // Hash function for OT
    static uint64_t hash64(int index, uint64_t v);

    // Hash function for OT with 128-bit input (for IKNP columns)
    static uint64_t hash64(int index, const U128& v);

    // Fast AES-based hash for OT (correlation-robust, not cryptographic)
    // Much faster than SHA-256, sufficient for IKNP OT extension
    static uint64_t hash64Fast(int index, const U128& v);

    // Batch hash for packed-bits IKNP: computes hashes for a limb (64 consecutive bits)
    // Takes 64 column values and returns a 64-bit result where bit i = LSB(hash(columns[i]))
    static uint64_t hashBatchForBits(int baseIndex, const U128* columns, const size_t* colIndices, size_t count);

    // Fast batch hash using AES for packed-bits IKNP
    static uint64_t hashBatchForBitsFast(int baseIndex, const U128* columns, const size_t* colIndices, size_t count);

    // Ultra-fast: hash entire tile and return packed bits for each limb
    // Processes all 128 columns of a transposed tile, returns hash LSBs packed into result array
    // Much faster than calling hash64Fast 128 times
    static void hashTileBatch(int baseIndex, const U128* tile, size_t validCount, uint64_t* hashBits);

    // 128x128 bit matrix transpose (in-place)
    static void transpose128x128_inplace(U128 v[128]);

    // Full transpose for 128xN blocks
    static void transpose128xNBlocks_full(const std::array<std::vector<U128>, 128> &rows,
                                         std::vector<U128> &cols,
                                         size_t blocks);

    // Fast column extraction (column 0 and column 64 only, no full transpose)
    static U128 extract_col0_from_rows(const U128 rows[128]);
    static U128 extract_col64_from_rows(const U128 rows[128]);

    // Extract column 0 and 64 from 128xN blocks (fast path)
    static void extract128xNBlocks_col0_col64_fast(
        const U128 *rows, size_t blocks,
        U128 *outCol0, U128 *outCol64
    );

    // Extract from raw uint64_t layout used in OT receiver
    static void extract_uRaw_col0_col64_fast(
        const uint64_t *uRaw, size_t blocks,
        U128 *outCol0, U128 *outCol64
    );
};

#endif

