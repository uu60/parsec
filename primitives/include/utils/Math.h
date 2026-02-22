
#ifndef MPC_PACKAGE_MATH_H
#define MPC_PACKAGE_MATH_H

#include <string>
#include <bitset>
#include <vector>
#include <cstdint>

// Forward declarations for OpenSSL types (avoid including OpenSSL headers in header)
typedef struct evp_pkey_st EVP_PKEY;
typedef struct bignum_st BIGNUM;
typedef struct bignum_ctx BN_CTX;

/**
 * RAII wrapper for RSA key using EVP_PKEY (OpenSSL 3.0+ compatible).
 * Manages key lifecycle and provides access to n, e, d components.
 */
class RsaKeyContext {
public:
    EVP_PKEY* pkey = nullptr;
    BIGNUM* n = nullptr;   // owned, must be freed
    BIGNUM* e = nullptr;   // owned, must be freed
    BIGNUM* d = nullptr;   // owned, must be freed (nullptr for public key)
    int nbytes = 0;

    RsaKeyContext() = default;
    ~RsaKeyContext();

    // Non-copyable
    RsaKeyContext(const RsaKeyContext&) = delete;
    RsaKeyContext& operator=(const RsaKeyContext&) = delete;

    // Movable
    RsaKeyContext(RsaKeyContext&& other) noexcept;
    RsaKeyContext& operator=(RsaKeyContext&& other) noexcept;
};

class Math {
public:
    static int64_t randInt();

    static int64_t randInt(int64_t lowest, int64_t highest);

    static int64_t pow(int64_t base, int64_t exponent);

    static std::string randString(int bytes);

    static std::string add(const std::string &add0, int64_t add1);

    static std::string add(const std::string &add0, const std::string &add1);

    static std::string minus(const std::string &add0, const std::string &add1);

    static int64_t ring(int64_t num, int width);

    static bool getBit(int64_t v, int i);

    static int64_t changeBit(int64_t v, int i, bool b);

    template<int width>
    static std::string toBinString(int64_t v) {
        return std::bitset<width>(v).to_string();
    };

    // ========== RSA-OT Helper Functions ==========

    /**
     * Load RSA public key from PEM string.
     * @param pem_str PEM-encoded public key
     * @return RsaKeyContext with n, e set (d is nullptr)
     * @throws std::runtime_error on failure
     */
    static RsaKeyContext loadRsaPublicKey(const std::string& pem_str);

    /**
     * Load RSA private key from PEM string.
     * @param pem_str PEM-encoded private key
     * @return RsaKeyContext with n, e, d all set
     * @throws std::runtime_error on failure
     */
    static RsaKeyContext loadRsaPrivateKey(const std::string& pem_str);

    /**
     * Convert bytes to fixed-length big-endian byte string (zero-padded).
     * @param bytes Input byte string (big-endian BIGNUM representation)
     * @param nbytes Target fixed length
     * @return Fixed-length byte string
     * @throws std::runtime_error on failure
     */
    static std::string toFixedBytes(const std::string& bytes, int nbytes);

    /**
     * Sample a random element in Z*_N (non-zero and coprime with n).
     * @param ctx RSA key context providing modulus n
     * @return Random element as fixed-length byte string
     * @throws std::runtime_error on failure
     */
    static std::string sampleZStarN(const RsaKeyContext& ctx);

    /**
     * Compute k^e mod n (RSA public key operation).
     * @param k_bytes Input k as byte string
     * @param ctx RSA key context with n, e
     * @return Result as fixed-length byte string
     * @throws std::runtime_error on failure
     */
    static std::string modExp(const std::string& base_bytes, const std::string& exp_bytes,
                              const RsaKeyContext& ctx);

    /**
     * Compute (a * b) mod n.
     * @param a_bytes First operand as byte string
     * @param b_bytes Second operand as byte string
     * @param ctx RSA key context with n
     * @return Result as fixed-length byte string
     * @throws std::runtime_error on failure
     */
    static std::string modMul(const std::string& a_bytes, const std::string& b_bytes,
                              const RsaKeyContext& ctx);

    /**
     * Compute modular inverse: a^(-1) mod n.
     * @param a_bytes Input as byte string
     * @param ctx RSA key context with n
     * @return Result as fixed-length byte string
     * @throws std::runtime_error if inverse doesn't exist
     */
    static std::string modInverse(const std::string& a_bytes, const RsaKeyContext& ctx);

    /**
     * Compute v^d mod n (RSA private key decryption).
     * @param v_bytes Input v as byte string
     * @param ctx RSA key context with n, d
     * @return Result as fixed-length byte string
     * @throws std::runtime_error on failure
     */
    static std::string rsaDecrypt(const std::string& v_bytes, const RsaKeyContext& ctx);

    /**
     * KDF using SHA256: compute SHA256 and return first 8 bytes as uint64_t.
     * @param input Input byte string
     * @return First 8 bytes of SHA256 digest as uint64_t
     * @throws std::runtime_error on failure
     */
    static uint64_t kdfSha256To8Bytes(const std::string& input);

    /**
     * Get the public exponent 'e' as byte string from RSA context.
     * @param ctx RSA key context
     * @return e as fixed-length byte string
     */
    static std::string getExponentE(const RsaKeyContext& ctx);

    /**
     * Get the private exponent 'd' as byte string from RSA context.
     * @param ctx RSA key context (must have d set)
     * @return d as fixed-length byte string
     * @throws std::runtime_error if d is not available
     */
    static std::string getExponentD(const RsaKeyContext& ctx);
};


#endif
