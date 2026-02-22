
#include "utils/Math.h"
#include <random>
#include <string>
#include <limits>
#include <iomanip>
#include <cstring>
#include <stdexcept>
#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/sha.h>
#include <openssl/core_names.h>
#include <openssl/param_build.h>

// ========== RsaKeyContext Implementation ==========

RsaKeyContext::~RsaKeyContext() {
    if (pkey != nullptr) {
        EVP_PKEY_free(pkey);
        pkey = nullptr;
    }
    if (n != nullptr) {
        BN_free(n);
        n = nullptr;
    }
    if (e != nullptr) {
        BN_free(e);
        e = nullptr;
    }
    if (d != nullptr) {
        BN_free(d);
        d = nullptr;
    }
}

RsaKeyContext::RsaKeyContext(RsaKeyContext&& other) noexcept
    : pkey(other.pkey), n(other.n), e(other.e), d(other.d), nbytes(other.nbytes) {
    other.pkey = nullptr;
    other.n = nullptr;
    other.e = nullptr;
    other.d = nullptr;
    other.nbytes = 0;
}

RsaKeyContext& RsaKeyContext::operator=(RsaKeyContext&& other) noexcept {
    if (this != &other) {
        if (pkey != nullptr) {
            EVP_PKEY_free(pkey);
        }
        if (n != nullptr) {
            BN_free(n);
        }
        if (e != nullptr) {
            BN_free(e);
        }
        if (d != nullptr) {
            BN_free(d);
        }
        pkey = other.pkey;
        n = other.n;
        e = other.e;
        d = other.d;
        nbytes = other.nbytes;
        other.pkey = nullptr;
        other.n = nullptr;
        other.e = nullptr;
        other.d = nullptr;
        other.nbytes = 0;
    }
    return *this;
}

// ========== Internal Helper Functions ==========

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
    BN_set_word(bn_add1, add1);
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
    return randInt(0, INT64_MAX);
}

thread_local std::mt19937_64 generator(std::random_device{}());

int64_t Math::randInt(int64_t lowest, int64_t highest) {
    std::uniform_int_distribution<int64_t> dist(lowest, highest);

    return dist(generator);
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
    std::uniform_int_distribution<int8_t> dis(-128, 127);

    std::vector<int8_t> temp;
    temp.reserve(bytes);
    for (size_t i = 0; i < bytes; ++i) {
        temp.push_back(dis(generator));
    }

    return std::string(temp.begin(), temp.end());
}

std::string Math::add(const std::string &add0, const std::string &add1) {
    return add_or_minus(add0, add1, false);
}

std::string Math::minus(const std::string &add0, const std::string &add1) {
    return add_or_minus(add0, add1, true);
}

int64_t Math::ring(int64_t num, int width) {
    if (width == 64) {
        return num;
    }
    return num & ((1LL << width) - 1);
}

bool Math::getBit(int64_t v, int i) {
    return (v >> i) & 1;
}

int64_t Math::changeBit(int64_t v, int i, bool b) {
    int64_t mask = ~(1LL << i);
    return (v & mask) | ((b ? 1LL : 0LL) << i);
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

// ========== RSA-OT Helper Functions Implementation ==========

RsaKeyContext Math::loadRsaPublicKey(const std::string& pem_str) {
    BIO* bio = BIO_new_mem_buf(pem_str.data(), static_cast<int>(pem_str.size()));
    if (bio == nullptr) {
        throw std::runtime_error("loadRsaPublicKey: BIO_new_mem_buf failed");
    }

    // Use EVP API (OpenSSL 3.0+)
    EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (pkey == nullptr) {
        throw std::runtime_error("loadRsaPublicKey: PEM_read_bio_PUBKEY failed");
    }

    // Verify it's an RSA key
    if (EVP_PKEY_base_id(pkey) != EVP_PKEY_RSA) {
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPublicKey: not an RSA key");
    }

    RsaKeyContext ctx;
    ctx.pkey = pkey;

    // Extract n and e using EVP_PKEY_get_bn_param (returns copies we own)
    if (!EVP_PKEY_get_bn_param(pkey, OSSL_PKEY_PARAM_RSA_N, &ctx.n)) {
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPublicKey: failed to get RSA n");
    }
    if (!EVP_PKEY_get_bn_param(pkey, OSSL_PKEY_PARAM_RSA_E, &ctx.e)) {
        BN_free(ctx.n);
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPublicKey: failed to get RSA e");
    }
    ctx.d = nullptr;
    ctx.nbytes = BN_num_bytes(ctx.n);
    return ctx;
}

RsaKeyContext Math::loadRsaPrivateKey(const std::string& pem_str) {
    BIO* bio = BIO_new_mem_buf(pem_str.data(), static_cast<int>(pem_str.size()));
    if (bio == nullptr) {
        throw std::runtime_error("loadRsaPrivateKey: BIO_new_mem_buf failed");
    }

    // Use EVP API (OpenSSL 3.0+)
    EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (pkey == nullptr) {
        throw std::runtime_error("loadRsaPrivateKey: PEM_read_bio_PrivateKey failed");
    }

    // Verify it's an RSA key
    if (EVP_PKEY_base_id(pkey) != EVP_PKEY_RSA) {
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPrivateKey: not an RSA key");
    }

    RsaKeyContext ctx;
    ctx.pkey = pkey;

    // Extract n, e, d using EVP_PKEY_get_bn_param (returns copies we own)
    if (!EVP_PKEY_get_bn_param(pkey, OSSL_PKEY_PARAM_RSA_N, &ctx.n)) {
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPrivateKey: failed to get RSA n");
    }
    if (!EVP_PKEY_get_bn_param(pkey, OSSL_PKEY_PARAM_RSA_E, &ctx.e)) {
        BN_free(ctx.n);
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPrivateKey: failed to get RSA e");
    }
    if (!EVP_PKEY_get_bn_param(pkey, OSSL_PKEY_PARAM_RSA_D, &ctx.d)) {
        BN_free(ctx.n);
        BN_free(ctx.e);
        EVP_PKEY_free(pkey);
        throw std::runtime_error("loadRsaPrivateKey: failed to get RSA d");
    }
    ctx.nbytes = BN_num_bytes(ctx.n);
    return ctx;
}

std::string Math::toFixedBytes(const std::string& bytes, int nbytes) {
    if (nbytes <= 0) {
        throw std::runtime_error("toFixedBytes: invalid nbytes");
    }
    if (static_cast<int>(bytes.size()) == nbytes) {
        return bytes;
    }
    if (static_cast<int>(bytes.size()) > nbytes) {
        // Truncate leading zeros if oversized
        size_t start = bytes.size() - nbytes;
        return bytes.substr(start, nbytes);
    }
    // Pad with leading zeros
    std::string result(nbytes - bytes.size(), '\0');
    result.append(bytes);
    return result;
}

std::string Math::sampleZStarN(const RsaKeyContext& ctx) {
    if (ctx.n == nullptr) {
        throw std::runtime_error("sampleZStarN: null modulus n");
    }

    BN_CTX* bn_ctx = BN_CTX_new();
    if (bn_ctx == nullptr) {
        throw std::runtime_error("sampleZStarN: BN_CTX_new failed");
    }

    BIGNUM* x = BN_new();
    BIGNUM* gcd = BN_new();
    if (x == nullptr || gcd == nullptr) {
        BN_free(x);
        BN_free(gcd);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("sampleZStarN: BN_new failed");
    }

    bool valid = false;
    while (!valid) {
        if (!BN_rand_range(x, ctx.n)) {
            BN_free(x);
            BN_free(gcd);
            BN_CTX_free(bn_ctx);
            throw std::runtime_error("sampleZStarN: BN_rand_range failed");
        }
        if (BN_is_zero(x)) continue;
        if (!BN_gcd(gcd, x, ctx.n, bn_ctx)) {
            BN_free(x);
            BN_free(gcd);
            BN_CTX_free(bn_ctx);
            throw std::runtime_error("sampleZStarN: BN_gcd failed");
        }
        if (BN_is_one(gcd)) valid = true;
    }

    std::string result(ctx.nbytes, '\0');
    int ret = BN_bn2binpad(x, reinterpret_cast<unsigned char*>(&result[0]), ctx.nbytes);
    BN_free(x);
    BN_free(gcd);
    BN_CTX_free(bn_ctx);

    if (ret != ctx.nbytes) {
        throw std::runtime_error("sampleZStarN: BN_bn2binpad failed");
    }
    return result;
}

std::string Math::modExp(const std::string& base_bytes, const std::string& exp_bytes,
                         const RsaKeyContext& ctx) {
    if (ctx.n == nullptr) {
        throw std::runtime_error("modExp: null modulus n");
    }

    BIGNUM* base = BN_bin2bn(reinterpret_cast<const unsigned char*>(base_bytes.data()),
                              static_cast<int>(base_bytes.size()), nullptr);
    BIGNUM* exp = BN_bin2bn(reinterpret_cast<const unsigned char*>(exp_bytes.data()),
                             static_cast<int>(exp_bytes.size()), nullptr);
    if (base == nullptr || exp == nullptr) {
        BN_free(base);
        BN_free(exp);
        throw std::runtime_error("modExp: BN_bin2bn failed");
    }

    BN_CTX* bn_ctx = BN_CTX_new();
    BIGNUM* result = BN_new();
    if (bn_ctx == nullptr || result == nullptr) {
        BN_free(base);
        BN_free(exp);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("modExp: allocation failed");
    }

    if (!BN_mod_exp(result, base, exp, ctx.n, bn_ctx)) {
        BN_free(base);
        BN_free(exp);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("modExp: BN_mod_exp failed");
    }

    std::string out(ctx.nbytes, '\0');
    int ret = BN_bn2binpad(result, reinterpret_cast<unsigned char*>(&out[0]), ctx.nbytes);
    BN_free(base);
    BN_free(exp);
    BN_free(result);
    BN_CTX_free(bn_ctx);

    if (ret != ctx.nbytes) {
        throw std::runtime_error("modExp: BN_bn2binpad failed");
    }
    return out;
}

std::string Math::modMul(const std::string& a_bytes, const std::string& b_bytes,
                         const RsaKeyContext& ctx) {
    if (ctx.n == nullptr) {
        throw std::runtime_error("modMul: null modulus n");
    }

    BIGNUM* a = BN_bin2bn(reinterpret_cast<const unsigned char*>(a_bytes.data()),
                           static_cast<int>(a_bytes.size()), nullptr);
    BIGNUM* b = BN_bin2bn(reinterpret_cast<const unsigned char*>(b_bytes.data()),
                           static_cast<int>(b_bytes.size()), nullptr);
    if (a == nullptr || b == nullptr) {
        BN_free(a);
        BN_free(b);
        throw std::runtime_error("modMul: BN_bin2bn failed");
    }

    BN_CTX* bn_ctx = BN_CTX_new();
    BIGNUM* result = BN_new();
    if (bn_ctx == nullptr || result == nullptr) {
        BN_free(a);
        BN_free(b);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("modMul: allocation failed");
    }

    if (!BN_mod_mul(result, a, b, ctx.n, bn_ctx)) {
        BN_free(a);
        BN_free(b);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("modMul: BN_mod_mul failed");
    }

    std::string out(ctx.nbytes, '\0');
    int ret = BN_bn2binpad(result, reinterpret_cast<unsigned char*>(&out[0]), ctx.nbytes);
    BN_free(a);
    BN_free(b);
    BN_free(result);
    BN_CTX_free(bn_ctx);

    if (ret != ctx.nbytes) {
        throw std::runtime_error("modMul: BN_bn2binpad failed");
    }
    return out;
}

std::string Math::modInverse(const std::string& a_bytes, const RsaKeyContext& ctx) {
    if (ctx.n == nullptr) {
        throw std::runtime_error("modInverse: null modulus n");
    }

    BIGNUM* a = BN_bin2bn(reinterpret_cast<const unsigned char*>(a_bytes.data()),
                           static_cast<int>(a_bytes.size()), nullptr);
    if (a == nullptr) {
        throw std::runtime_error("modInverse: BN_bin2bn failed");
    }

    BN_CTX* bn_ctx = BN_CTX_new();
    BIGNUM* result = BN_new();
    if (bn_ctx == nullptr || result == nullptr) {
        BN_free(a);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("modInverse: allocation failed");
    }

    if (BN_mod_inverse(result, a, ctx.n, bn_ctx) == nullptr) {
        BN_free(a);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("modInverse: BN_mod_inverse failed - no inverse exists");
    }

    std::string out(ctx.nbytes, '\0');
    int ret = BN_bn2binpad(result, reinterpret_cast<unsigned char*>(&out[0]), ctx.nbytes);
    BN_free(a);
    BN_free(result);
    BN_CTX_free(bn_ctx);

    if (ret != ctx.nbytes) {
        throw std::runtime_error("modInverse: BN_bn2binpad failed");
    }
    return out;
}

std::string Math::rsaDecrypt(const std::string& v_bytes, const RsaKeyContext& ctx) {
    if (ctx.n == nullptr || ctx.d == nullptr) {
        throw std::runtime_error("rsaDecrypt: missing n or d in context");
    }

    BIGNUM* v = BN_bin2bn(reinterpret_cast<const unsigned char*>(v_bytes.data()),
                           static_cast<int>(v_bytes.size()), nullptr);
    if (v == nullptr) {
        throw std::runtime_error("rsaDecrypt: BN_bin2bn failed");
    }

    BN_CTX* bn_ctx = BN_CTX_new();
    BIGNUM* result = BN_new();
    if (bn_ctx == nullptr || result == nullptr) {
        BN_free(v);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("rsaDecrypt: allocation failed");
    }

    // Compute v^d mod n
    if (!BN_mod_exp(result, v, ctx.d, ctx.n, bn_ctx)) {
        BN_free(v);
        BN_free(result);
        BN_CTX_free(bn_ctx);
        throw std::runtime_error("rsaDecrypt: BN_mod_exp failed");
    }

    std::string out(ctx.nbytes, '\0');
    int ret = BN_bn2binpad(result, reinterpret_cast<unsigned char*>(&out[0]), ctx.nbytes);
    BN_free(v);
    BN_free(result);
    BN_CTX_free(bn_ctx);

    if (ret != ctx.nbytes) {
        throw std::runtime_error("rsaDecrypt: BN_bn2binpad failed");
    }
    return out;
}

uint64_t Math::kdfSha256To8Bytes(const std::string& input) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if (SHA256(reinterpret_cast<const unsigned char*>(input.data()),
               input.size(), digest) == nullptr) {
        throw std::runtime_error("kdfSha256To8Bytes: SHA256 failed");
    }
    uint64_t result = 0;
    std::memcpy(&result, digest, sizeof(uint64_t));
    return result;
}

std::string Math::getExponentE(const RsaKeyContext& ctx) {
    if (ctx.e == nullptr) {
        throw std::runtime_error("getExponentE: null exponent e");
    }
    int e_bytes = BN_num_bytes(ctx.e);
    std::string out(e_bytes, '\0');
    int ret = BN_bn2bin(ctx.e, reinterpret_cast<unsigned char*>(&out[0]));
    if (ret != e_bytes) {
        throw std::runtime_error("getExponentE: BN_bn2bin failed");
    }
    return out;
}

std::string Math::getExponentD(const RsaKeyContext& ctx) {
    if (ctx.d == nullptr) {
        throw std::runtime_error("getExponentD: null exponent d - not a private key");
    }
    std::string out(ctx.nbytes, '\0');
    int ret = BN_bn2binpad(ctx.d, reinterpret_cast<unsigned char*>(&out[0]), ctx.nbytes);
    if (ret != ctx.nbytes) {
        throw std::runtime_error("getExponentD: BN_bn2binpad failed");
    }
    return out;
}

