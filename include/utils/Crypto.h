//
// Created by 杜建璋 on 2024/7/18.
//

#ifndef MPC_PACKAGE_CRYPTO_H
#define MPC_PACKAGE_CRYPTO_H
#include <iostream>
#include <unordered_map>

class Crypto {
public:
    static std::unordered_map<int, std::string> _selfPubs;
    static std::unordered_map<int, std::string> _selfPris;
    static std::unordered_map<int, std::string> _otherPubs;

    // true if generated else existed
    static bool generateRsaKeys(int bits);

    static std::string rsaEncrypt(const std::string &data, const std::string &publicKey);

    static std::string rsaDecrypt(const std::string &encryptedData, const std::string &privateKey);
};


#endif //MPC_PACKAGE_CRYPTO_H
