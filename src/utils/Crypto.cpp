//
// Created by 杜建璋 on 2024/7/18.
//

#include "utils/Crypto.h"
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <vector>
#include <string>

std::unordered_map<int, std::string> Crypto::_selfPubs = {};
std::unordered_map<int, std::string> Crypto::_selfPris = {};
std::unordered_map<int, std::string> Crypto::_otherPubs = {};

// copilot
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

// copilot
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

// copilot
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


