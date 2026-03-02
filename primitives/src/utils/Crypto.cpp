//
// #include "utils/Crypto.h"
// #include <openssl/rsa.h>
// #include <openssl/pem.h>
// #include <vector>
// #include <string>
//
// std::unordered_map<int, std::string> Crypto::_selfPubs = {};
// std::unordered_map<int, std::string> Crypto::_selfPris = {};
// std::unordered_map<int, std::string> Crypto::_otherPubs = {};
//
// bool Crypto::generateRsaKeys(int bits) {
//     if (_selfPubs.count(bits) > 0) {
//         return false;
//     }
//
//     EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
//     EVP_PKEY *pkey = nullptr;
//     EVP_PKEY_keygen_init(ctx);
//     EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, bits);
//     EVP_PKEY_keygen(ctx, &pkey);
//
//     BIO *pri = BIO_new(BIO_s_mem());
//     BIO *pub = BIO_new(BIO_s_mem());
//
//     PEM_write_bio_PrivateKey(pri, pkey, nullptr, nullptr, 0, nullptr, nullptr);
//     PEM_write_bio_PUBKEY(pub, pkey);
//
//     size_t pri_len = BIO_pending(pri);
//     size_t pub_len = BIO_pending(pub);
//
//     char *pri_key = (char *)malloc(pri_len + 1);
//     char *pub_key = (char *)malloc(pub_len + 1);
//
//     BIO_read(pri, pri_key, pri_len);
//     BIO_read(pub, pub_key, pub_len);
//
//     pri_key[pri_len] = '\0';
//     pub_key[pub_len] = '\0';
//
//     _selfPubs[bits] = std::string(pub_key);
//     _selfPris[bits] = std::string(pri_key);
//     EVP_PKEY_free(pkey);
//     BIO_free_all(pub);
//     BIO_free_all(pri);
//     free(pri_key);
//     free(pub_key);
//
//     return true;
// }
//
// std::string Crypto::rsaEncrypt(const std::string &data, const std::string &publicKey) {
//     EVP_PKEY *pkey = nullptr;
//     EVP_PKEY_CTX *ctx = nullptr;
//     BIO *keybio = BIO_new_mem_buf((void *)publicKey.c_str(), -1);
//     pkey = PEM_read_bio_PUBKEY(keybio, nullptr, nullptr, nullptr);
//     ctx = EVP_PKEY_CTX_new(pkey, nullptr);
//     EVP_PKEY_encrypt_init(ctx);
//
//     size_t outlen;
//     EVP_PKEY_encrypt(ctx, nullptr, &outlen, (unsigned char *)data.c_str(), data.size());
//     std::vector<unsigned char> outbuf(outlen);
//     EVP_PKEY_encrypt(ctx, outbuf.data(), &outlen, (unsigned char *)data.c_str(), data.size());
//
//     EVP_PKEY_CTX_free(ctx);
//     EVP_PKEY_free(pkey);
//     BIO_free(keybio);
//     return std::string(outbuf.begin(), outbuf.end());
// }
//
// std::string Crypto::rsaDecrypt(const std::string &encryptedData, const std::string &privateKey) {
//     EVP_PKEY *pkey = nullptr;
//     EVP_PKEY_CTX *ctx = nullptr;
//     BIO *keybio = BIO_new_mem_buf((void *)privateKey.c_str(), -1);
//     pkey = PEM_read_bio_PrivateKey(keybio, nullptr, nullptr, nullptr);
//     ctx = EVP_PKEY_CTX_new(pkey, nullptr);
//     EVP_PKEY_decrypt_init(ctx);
//
//     size_t outlen;
//     EVP_PKEY_decrypt(ctx, nullptr, &outlen, (unsigned char *)encryptedData.c_str(), encryptedData.size());
//     std::string decryptedStr(outlen, '\0');
//     EVP_PKEY_decrypt(ctx, (unsigned char *)decryptedStr.data(), &outlen, (unsigned char *)encryptedData.c_str(), encryptedData.size());
//     decryptedStr.resize(outlen);
//
//     EVP_PKEY_CTX_free(ctx);
//     EVP_PKEY_free(pkey);
//     BIO_free(keybio);
//     return decryptedStr;
// }
//
//


#include "utils/Crypto.h"
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <vector>
#include <string>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>

std::unordered_map<int, std::string> Crypto::_selfPubs = {};
std::unordered_map<int, std::string> Crypto::_selfPris = {};
std::unordered_map<int, std::string> Crypto::_otherPubs = {};


static inline void store_u64_le(uint64_t x, unsigned char out[8]) {
    out[0] = (unsigned char)(x);
    out[1] = (unsigned char)(x >> 8);
    out[2] = (unsigned char)(x >> 16);
    out[3] = (unsigned char)(x >> 24);
    out[4] = (unsigned char)(x >> 32);
    out[5] = (unsigned char)(x >> 40);
    out[6] = (unsigned char)(x >> 48);
    out[7] = (unsigned char)(x >> 56);
}

auto h = [](uint64_t in) -> uint64_t {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    unsigned char buf[8];
    store_u64_le(in, buf);

    SHA256(buf, sizeof(buf), digest);

    uint64_t out = 0;
    std::memcpy(&out, digest, sizeof(out)); // first 8 bytes as u64
    return out;
};

bool Crypto::generateRsaKeys(int bits) {
    if (_selfPubs.count(bits) > 0) {
        return false;
    }

    EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
    EVP_PKEY *pkey = nullptr;
    EVP_PKEY_keygen_init(ctx);
    EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, bits);
    EVP_PKEY_keygen(ctx, &pkey);

    BIO *pri = BIO_new(BIO_s_mem());
    BIO *pub = BIO_new(BIO_s_mem());

    PEM_write_bio_PrivateKey(pri, pkey, nullptr, nullptr, 0, nullptr, nullptr);
    PEM_write_bio_PUBKEY(pub, pkey);

    size_t pri_len = BIO_pending(pri);
    size_t pub_len = BIO_pending(pub);

    char *pri_key = (char *)malloc(pri_len + 1);
    char *pub_key = (char *)malloc(pub_len + 1);

    BIO_read(pri, pri_key, pri_len);
    BIO_read(pub, pub_key, pub_len);

    pri_key[pri_len] = '\0';
    pub_key[pub_len] = '\0';

    _selfPubs[bits] = std::string(pub_key);
    _selfPris[bits] = std::string(pri_key);
    EVP_PKEY_free(pkey);
    BIO_free_all(pub);
    BIO_free_all(pri);
    free(pri_key);
    free(pub_key);

    return true;
}

std::string Crypto::rsaEncrypt(const std::string &data, const std::string &publicKey) {
    EVP_PKEY *pkey = nullptr;
    EVP_PKEY_CTX *ctx = nullptr;
    BIO *keybio = BIO_new_mem_buf((void *)publicKey.c_str(), -1);
    pkey = PEM_read_bio_PUBKEY(keybio, nullptr, nullptr, nullptr);
    ctx = EVP_PKEY_CTX_new(pkey, nullptr);
    EVP_PKEY_encrypt_init(ctx);

    size_t outlen;
    EVP_PKEY_encrypt(ctx, nullptr, &outlen, (unsigned char *)data.c_str(), data.size());
    std::vector<unsigned char> outbuf(outlen);
    EVP_PKEY_encrypt(ctx, outbuf.data(), &outlen, (unsigned char *)data.c_str(), data.size());

    EVP_PKEY_CTX_free(ctx);
    EVP_PKEY_free(pkey);
    BIO_free(keybio);
    return std::string(outbuf.begin(), outbuf.end());
}

std::string Crypto::rsaDecrypt(const std::string &encryptedData, const std::string &privateKey) {
    EVP_PKEY *pkey = nullptr;
    EVP_PKEY_CTX *ctx = nullptr;
    BIO *keybio = BIO_new_mem_buf((void *)privateKey.c_str(), -1);
    pkey = PEM_read_bio_PrivateKey(keybio, nullptr, nullptr, nullptr);
    ctx = EVP_PKEY_CTX_new(pkey, nullptr);
    EVP_PKEY_decrypt_init(ctx);

    size_t outlen;
    EVP_PKEY_decrypt(ctx, nullptr, &outlen, (unsigned char *)encryptedData.c_str(), encryptedData.size());
    std::string decryptedStr(outlen, '\0');
    EVP_PKEY_decrypt(ctx, (unsigned char *)decryptedStr.data(), &outlen, (unsigned char *)encryptedData.c_str(), encryptedData.size());
    decryptedStr.resize(outlen);

    EVP_PKEY_CTX_free(ctx);
    EVP_PKEY_free(pkey);
    BIO_free(keybio);
    return decryptedStr;
}

// ============== AES-CTR PRG Implementation ==============

Crypto::AesCtrPrg::AesCtrPrg(uint64_t seed) {
    uint64_t k0 = h(seed ^ 0x9e3779b97f4a7c15ULL);
    uint64_t k1 = h(seed ^ 0xbf58476d1ce4e5b9ULL);
    std::memcpy(_key.data(), &k0, 8);
    std::memcpy(_key.data() + 8, &k1, 8);
    _iv.fill(0);
}

Crypto::AesCtrPrg::~AesCtrPrg() = default;

void Crypto::AesCtrPrg::generateBlocks(U128 *out, size_t blocks) const {
    if (blocks == 0) return;

    std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>
            ctx(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    if (!ctx) {
        throw std::runtime_error("EVP_CIPHER_CTX_new failed");
    }

    if (EVP_EncryptInit_ex(ctx.get(), EVP_aes_128_ctr(), nullptr, _key.data(), _iv.data()) != 1) {
        throw std::runtime_error("EVP_EncryptInit_ex failed");
    }

    unsigned char *bytes = reinterpret_cast<unsigned char *>(out);
    const size_t nbytes = blocks * 16;

    std::memset(bytes, 0, nbytes);

    int outLen = 0;
    if (EVP_EncryptUpdate(ctx.get(), bytes, &outLen, bytes, static_cast<int>(nbytes)) != 1) {
        throw std::runtime_error("EVP_EncryptUpdate failed");
    }
}

void Crypto::batchPrgGenerate(const uint64_t* seeds, size_t numSeeds,
                               U128* out, size_t blocksPerSeed) {
    if (numSeeds == 0 || blocksPerSeed == 0) return;

    // ---------------------------------------------------------------
    // Fixed-key AES-ECB PRG:
    //   out[seed * blocksPerSeed + blk] = AES_K(seed || blk) XOR (seed || blk)
    //
    // Benefits over per-seed AES-CTR:
    //  1. Only ONE EVP_EncryptInit_ex call per batchPrgGenerate invocation
    //     (key schedule loaded once, not once-per-seed).
    //  2. All (numSeeds * blocksPerSeed) blocks are encrypted in a single
    //     EVP_EncryptUpdate call, maximising AES-NI pipeline utilisation.
    //  3. Davies-Meyer XOR gives correlation-robustness for OT security.
    // ---------------------------------------------------------------

    // Fixed PRG key (domain-separated from hash key)
    static const unsigned char PRG_KEY[16] = {
        0x50, 0x52, 0x47, 0x5F,  // "PRG_"
        0x49, 0x4B, 0x4E, 0x50,  // "IKNP"
        0x4B, 0x45, 0x59, 0x32,  // "KEY2"
        0x00, 0x00, 0x00, 0x00
    };

    // Thread-local context initialised once with the fixed key
    thread_local struct PrgCtxHolder {
        EVP_CIPHER_CTX *ctx = nullptr;
        PrgCtxHolder() {
            ctx = EVP_CIPHER_CTX_new();
            if (ctx) {
                EVP_EncryptInit_ex(ctx, EVP_aes_128_ecb(), nullptr, PRG_KEY, nullptr);
                EVP_CIPHER_CTX_set_padding(ctx, 0);
            }
        }
        ~PrgCtxHolder() { if (ctx) EVP_CIPHER_CTX_free(ctx); }
    } holder;

    EVP_CIPHER_CTX *ctx = holder.ctx;
    if (!ctx) throw std::runtime_error("EVP_CIPHER_CTX_new failed");

    const size_t total = numSeeds * blocksPerSeed;

    // Build plaintext buffer: each block = (seed XOR derived_lo) || (block_idx XOR derived_hi)
    // We write directly into out[] then XOR in-place after encryption.
    unsigned char* buf = reinterpret_cast<unsigned char*>(out);
    for (size_t i = 0; i < numSeeds; ++i) {
        // Derive two 64-bit words from the seed for key mixing
        const uint64_t s0 = seeds[i] ^ 0x9e3779b97f4a7c15ULL;
        const uint64_t s1 = seeds[i] ^ 0xbf58476d1ce4e5b9ULL;
        for (size_t b = 0; b < blocksPerSeed; ++b) {
            const size_t off = (i * blocksPerSeed + b) * 16;
            const uint64_t lo = s0 + b;
            const uint64_t hi = s1 ^ (b * 0x6c62272e07bb0142ULL);
            std::memcpy(buf + off,     &lo, 8);
            std::memcpy(buf + off + 8, &hi, 8);
        }
    }

    // Encrypt all blocks in one call (maximises AES-NI throughput)
    int outLen = 0;
    if (EVP_EncryptUpdate(ctx, buf, &outLen, buf,
                          static_cast<int>(total * 16)) != 1) {
        throw std::runtime_error("EVP_EncryptUpdate failed in batchPrgGenerate");
    }

    // Davies-Meyer XOR: out[i] ^= plaintext[i]  (already in buf before encrypt)
    // We must re-derive the plaintext since we encrypted in-place.
    for (size_t i = 0; i < numSeeds; ++i) {
        const uint64_t s0 = seeds[i] ^ 0x9e3779b97f4a7c15ULL;
        const uint64_t s1 = seeds[i] ^ 0xbf58476d1ce4e5b9ULL;
        for (size_t b = 0; b < blocksPerSeed; ++b) {
            U128& blk = out[i * blocksPerSeed + b];
            blk.lo ^= s0 + b;
            blk.hi ^= s1 ^ (b * 0x6c62272e07bb0142ULL);
        }
    }
}

// ============== Hash Function ==============

uint64_t Crypto::hash64(int index, uint64_t v) {
    // Cryptographically secure hash for OT protocols
    // Uses SHA-256 and extracts first 64 bits
    unsigned char digest[SHA256_DIGEST_LENGTH];
    unsigned char input[16];

    // Pack index (4 bytes) + v (8 bytes) + domain separator (4 bytes)
    store_u64_le(v, input);
    std::memcpy(input + 8, &index, sizeof(int));

    // Domain separation constant to prevent hash collisions with other uses
    const uint32_t domain = 0x494B4E50; // "IKNP" in ASCII
    std::memcpy(input + 12, &domain, sizeof(uint32_t));

    SHA256(input, 16, digest);

    // Extract first 64 bits as little-endian
    uint64_t result;
    std::memcpy(&result, digest, sizeof(uint64_t));
    return result;
}

uint64_t Crypto::hash64(int index, const U128& v) {
    // Cryptographically secure hash for OT protocols with 128-bit input
    // Uses SHA-256 and extracts first 64 bits
    unsigned char digest[SHA256_DIGEST_LENGTH];
    unsigned char input[24];

    // Pack: v.lo (8 bytes) + v.hi (8 bytes) + index (4 bytes) + domain (4 bytes)
    store_u64_le(v.lo, input);
    store_u64_le(v.hi, input + 8);
    std::memcpy(input + 16, &index, sizeof(int));

    // Domain separation constant
    const uint32_t domain = 0x494B4E50; // "IKNP" in ASCII
    std::memcpy(input + 20, &domain, sizeof(uint32_t));

    SHA256(input, 24, digest);

    // Extract first 64 bits as little-endian
    uint64_t result;
    std::memcpy(&result, digest, sizeof(uint64_t));
    return result;
}

uint64_t Crypto::hashBatchForBits(int baseIndex, const U128* columns, const size_t* colIndices, size_t count) {
    // Batch hash for packed-bits IKNP
    // Returns a 64-bit result where bit i = LSB(hash(columns[colIndices[i]]))
    // This avoids per-bit function call overhead
    uint64_t result = 0;

    // Process in batch - still using SHA-256 but with reduced call overhead
    for (size_t i = 0; i < count; ++i) {
        const size_t colIdx = colIndices[i];
        const U128& col = columns[colIdx];
        const int index = baseIndex + static_cast<int>(i);

        // Inline the hash computation to avoid function call overhead
        unsigned char digest[SHA256_DIGEST_LENGTH];
        unsigned char input[24];

        store_u64_le(col.lo, input);
        store_u64_le(col.hi, input + 8);
        std::memcpy(input + 16, &index, sizeof(int));

        const uint32_t domain = 0x494B4E50; // "IKNP"
        std::memcpy(input + 20, &domain, sizeof(uint32_t));

        SHA256(input, 24, digest);

        // Extract just LSB
        if (digest[0] & 1) {
            result |= (1ULL << i);
        }
    }

    return result;
}

uint64_t Crypto::hash64Fast(int index, const U128& v) {
    // Ultra-fast correlation-robust hash using fixed-key AES
    // H(index, x) = AES_k(x XOR index_block) XOR x
    //
    // Key optimization: Use a single static EVP_CIPHER_CTX with fixed key,
    // and mix the index into the plaintext instead of the key.
    // This avoids key schedule computation on every call.

    // Fixed key for IKNP hashing
    static const unsigned char FIXED_KEY[16] = {
        0x49, 0x4B, 0x4E, 0x50,  // "IKNP"
        0x48, 0x41, 0x53, 0x48,  // "HASH"
        0x4B, 0x45, 0x59, 0x31,  // "KEY1"
        0x00, 0x00, 0x00, 0x00
    };

    // Thread-local AES context - initialized once per thread
    thread_local struct AesCtxHolder {
        EVP_CIPHER_CTX *ctx;
        AesCtxHolder() {
            ctx = EVP_CIPHER_CTX_new();
            EVP_EncryptInit_ex(ctx, EVP_aes_128_ecb(), nullptr, FIXED_KEY, nullptr);
            EVP_CIPHER_CTX_set_padding(ctx, 0);
        }
        ~AesCtxHolder() {
            if (ctx) EVP_CIPHER_CTX_free(ctx);
        }
    } holder;

    // Mix index into input for domain separation
    // Use XOR which is fast and provides good mixing
    alignas(16) unsigned char input[16];
    alignas(16) unsigned char output[16];

    const uint64_t index64 = static_cast<uint64_t>(static_cast<uint32_t>(index));
    const uint64_t lo_mixed = v.lo ^ index64;
    const uint64_t hi_mixed = v.hi ^ (index64 * 0x9e3779b97f4a7c15ULL);  // Golden ratio constant for better mixing

    store_u64_le(lo_mixed, input);
    store_u64_le(hi_mixed, input + 8);

    // Single AES block encryption - no re-init needed for ECB with same key
    int outlen;
    EVP_EncryptUpdate(holder.ctx, output, &outlen, input, 16);

    // XOR with original input (Davies-Meyer construction)
    uint64_t result;
    std::memcpy(&result, output, sizeof(uint64_t));
    result ^= v.lo;

    return result;
}

uint64_t Crypto::hashBatchForBitsFast(int baseIndex, const U128* columns, const size_t* colIndices, size_t count) {
    // Fast batch hash using AES for packed-bits IKNP
    // Returns a 64-bit result where bit i = LSB(hash(columns[colIndices[i]]))
    // Optimized: reuse AES context across the batch

    if (count == 0) return 0;

    uint64_t result = 0;

    // Create AES context once for the batch
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();

    alignas(16) unsigned char key[16];
    alignas(16) unsigned char input[16];
    alignas(16) unsigned char output[16];

    const uint32_t domain = 0x494B4E50; // "IKNP"

    for (size_t i = 0; i < count; ++i) {
        const size_t colIdx = colIndices[i];
        const U128& col = columns[colIdx];
        const int index = baseIndex + static_cast<int>(i);

        // Build key with index for domain separation
        std::memset(key, 0, 16);
        std::memcpy(key, &index, sizeof(int));
        std::memcpy(key + 4, &domain, sizeof(uint32_t));

        // Input is the column value
        store_u64_le(col.lo, input);
        store_u64_le(col.hi, input + 8);

        // Single-block AES
        EVP_EncryptInit_ex(ctx, EVP_aes_128_ecb(), nullptr, key, nullptr);
        EVP_CIPHER_CTX_set_padding(ctx, 0);

        int outlen;
        EVP_EncryptUpdate(ctx, output, &outlen, input, 16);

        // XOR with input (Davies-Meyer) and extract LSB
        uint64_t h;
        std::memcpy(&h, output, sizeof(uint64_t));
        h ^= col.lo;

        if (h & 1) {
            result |= (1ULL << i);
        }
    }

    EVP_CIPHER_CTX_free(ctx);
    return result;
}

void Crypto::hashTileBatch(int baseIndex, const U128* tile, size_t validCount, uint64_t* hashBits) {
    // Ultra-fast batch hash for entire tile.
    // Processes up to 128 columns; packs LSB of each hash into hashBits[0..1].
    //
    // Key optimisation: build all 128 (or validCount) input blocks in one
    // aligned buffer, then encrypt them all in a SINGLE EVP_EncryptUpdate call.
    // This lets AES-NI pipeline all blocks simultaneously instead of making
    // 128 individual calls.

    hashBits[0] = 0;
    hashBits[1] = 0;
    if (validCount == 0) return;

    static const unsigned char FIXED_KEY[16] = {
        0x49, 0x4B, 0x4E, 0x50,  // "IKNP"
        0x48, 0x41, 0x53, 0x48,  // "HASH"
        0x4B, 0x45, 0x59, 0x31,  // "KEY1"
        0x00, 0x00, 0x00, 0x00
    };

    thread_local struct AesCtxHolder {
        EVP_CIPHER_CTX *ctx;
        AesCtxHolder() {
            ctx = EVP_CIPHER_CTX_new();
            EVP_EncryptInit_ex(ctx, EVP_aes_128_ecb(), nullptr, FIXED_KEY, nullptr);
            EVP_CIPHER_CTX_set_padding(ctx, 0);
        }
        ~AesCtxHolder() { if (ctx) EVP_CIPHER_CTX_free(ctx); }
    } holder;

    const size_t count = std::min(validCount, static_cast<size_t>(128));

    // Pack all inputs into a contiguous aligned buffer
    alignas(16) unsigned char inputs[128 * 16];
    for (size_t k = 0; k < count; ++k) {
        const U128& col = tile[k];
        const uint64_t idx64 = static_cast<uint64_t>(static_cast<uint32_t>(baseIndex + static_cast<int>(k)));
        const uint64_t lo_mixed = col.lo ^ idx64;
        const uint64_t hi_mixed = col.hi ^ (idx64 * 0x9e3779b97f4a7c15ULL);
        std::memcpy(inputs + k * 16,     &lo_mixed, 8);
        std::memcpy(inputs + k * 16 + 8, &hi_mixed, 8);
    }

    // Encrypt all blocks in ONE call
    alignas(16) unsigned char outputs[128 * 16];
    int outlen = 0;
    EVP_EncryptUpdate(holder.ctx, outputs, &outlen, inputs,
                      static_cast<int>(count * 16));

    // Davies-Meyer XOR and extract LSB into hashBits
    for (size_t k = 0; k < count; ++k) {
        uint64_t h;
        std::memcpy(&h, outputs + k * 16, sizeof(uint64_t));
        const U128& col = tile[k];
        h ^= col.lo;  // Davies-Meyer

        if (h & 1) {
            if (k < 64) hashBits[0] |= (1ULL << k);
            else        hashBits[1] |= (1ULL << (k - 64));
        }
    }
}


void Crypto::transpose128x128_inplace(U128 v[128]) {
    uint64_t x0[128];
    uint64_t x1[128];
    for (int i = 0; i < 128; ++i) {
        x0[i] = v[i].lo;
        x1[i] = v[i].hi;
    }

    auto transpose64 = [](uint64_t x[128]) {
        static constexpr uint64_t m1 = 0x5555555555555555ULL;
        static constexpr uint64_t m2 = 0x3333333333333333ULL;
        static constexpr uint64_t m4 = 0x0f0f0f0f0f0f0f0fULL;
        static constexpr uint64_t m8 = 0x00ff00ff00ff00ffULL;
        static constexpr uint64_t m16 = 0x0000ffff0000ffffULL;
        static constexpr uint64_t m32 = 0x00000000ffffffffULL;

        auto swap_rows = [&](int shift, uint64_t mask) {
            for (int i = 0; i < 128; i += 2 * shift) {
                for (int j = 0; j < shift; ++j) {
                    uint64_t a = x[i + j];
                    uint64_t b = x[i + j + shift];
                    uint64_t t = ((a >> shift) ^ b) & mask;
                    b ^= t;
                    a ^= (t << shift);
                    x[i + j] = a;
                    x[i + j + shift] = b;
                }
            }
        };

        swap_rows(1, m1);
        swap_rows(2, m2);
        swap_rows(4, m4);
        swap_rows(8, m8);
        swap_rows(16, m16);
        swap_rows(32, m32);
    };

    transpose64(x0);
    transpose64(x1);

    for (int i = 0; i < 64; ++i) {
        std::swap(x0[i + 64], x1[i]);
    }

    for (int i = 0; i < 128; ++i) {
        v[i] = {x0[i], x1[i]};
    }
}

void Crypto::transpose128xNBlocks_full(const std::array<std::vector<U128>, 128> &rows,
                                      std::vector<U128> &cols,
                                      size_t blocks) {
    cols.assign(blocks * 128, U128{0, 0});

    U128 tile[128];
    for (size_t blk = 0; blk < blocks; ++blk) {
        for (int r = 0; r < 128; ++r) {
            tile[r] = rows[r][blk];
        }
        transpose128x128_inplace(tile);
        for (int bit = 0; bit < 128; ++bit) {
            cols[blk * 128 + static_cast<size_t>(bit)] = tile[bit];
        }
    }
}

// ============== Column Extraction Functions ==============

U128 Crypto::extract_col0_from_rows(const U128 rows[128]) {
    uint64_t lo = 0, hi = 0;
    for (int r = 0; r < 64; ++r) {
        lo |= (rows[r].lo & 1ULL) << r;
    }
    for (int r = 64; r < 128; ++r) {
        hi |= (rows[r].lo & 1ULL) << (r - 64);
    }
    return U128{lo, hi};
}

U128 Crypto::extract_col64_from_rows(const U128 rows[128]) {
    uint64_t lo = 0, hi = 0;
    for (int r = 0; r < 64; ++r) {
        lo |= (rows[r].hi & 1ULL) << r;
    }
    for (int r = 64; r < 128; ++r) {
        hi |= (rows[r].hi & 1ULL) << (r - 64);
    }
    return U128{lo, hi};
}

void Crypto::extract128xNBlocks_col0_col64_fast(
    const U128 *rows, size_t blocks,
    U128 *outCol0, U128 *outCol64
) {
    U128 tile[128];
    for (size_t blk = 0; blk < blocks; ++blk) {
        const U128 *base = rows + blk;
        for (int r = 0; r < 128; ++r) {
            tile[r] = base[static_cast<size_t>(r) * blocks];
        }
        outCol0[blk] = extract_col0_from_rows(tile);
        outCol64[blk] = extract_col64_from_rows(tile);
    }
}

void Crypto::extract_uRaw_col0_col64_fast(
    const uint64_t *uRaw, size_t blocks,
    U128 *outCol0, U128 *outCol64
) {
    U128 tile[128];

    for (size_t blk = 0; blk < blocks; ++blk) {
        for (int r = 0; r < 128; ++r) {
            const size_t off = (static_cast<size_t>(r) * blocks + blk) * 2;
            tile[r].lo = static_cast<uint64_t>(uRaw[off]);
            tile[r].hi = static_cast<uint64_t>(uRaw[off + 1]);
        }
        outCol0[blk] = extract_col0_from_rows(tile);
        outCol64[blk] = extract_col64_from_rows(tile);
    }
}

