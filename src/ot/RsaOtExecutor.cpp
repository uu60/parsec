//
// Created by 杜建璋 on 2024/7/15.
//

#include "ot/RsaOtExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"
#include "utils/Crypto.h"

RsaOtExecutor::RsaOtExecutor(int sender, int64_t m0, int64_t m1, int l, int i)
    : RsaOtExecutor(2048, sender, m0, m1, l, i) {
}

RsaOtExecutor::RsaOtExecutor(int bits, int sender, int64_t m0, int64_t m1, int l, int i) : SecureExecutor(l) {
    _bits = bits;
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _m0 = ring(m0);
        _m1 = ring(m1);
    } else {
        _i = i;
    }
}

RsaOtExecutor *RsaOtExecutor::execute() {
    // preparation
    generateAndShareRandoms();
    generateAndShareRsaKeys();

    // process
    process();
    return this;
}

void RsaOtExecutor::generateAndShareRsaKeys() {
    if (_isSender) {
        bool newKey = Crypto::generateRsaKeys(_bits);
        _pub = Crypto::_selfPubs[_bits];
        _pri = Crypto::_selfPris[_bits];
        if (newKey) {
            Comm::ssend(&_pub);
        }
        return;
    }
    // receiver
    if (Crypto::_otherPubs.count(_bits) > 0) {
        _pub = Crypto::_otherPubs[_bits];
    } else {
        Comm::srecv(&_pub);
        Crypto::_otherPubs[_bits] = _pub;
    }
}

void RsaOtExecutor::generateAndShareRandoms() {
    // 11 for PKCS#1 v1.5 padding
    int len = (_bits >> 3) - 11;
    if (_isSender) {
        _rand0 = Math::randString(len);
        _rand1 = Math::randString(len);
        Comm::ssend(&_rand0);
        Comm::ssend(&_rand1);
    } else {
        _randK = Math::randString(len);
        Comm::srecv(&_rand0);
        Comm::srecv(&_rand1);
    }
}

void RsaOtExecutor::process() {
    if (!_isSender) {
        std::string ek = Crypto::rsaEncrypt(_randK, _pub);
        std::string sumStr = Math::add(ek, _i == 0 ? _rand0 : _rand1);
        Comm::ssend(&sumStr);

        std::string m0, m1;
        Comm::srecv(&m0);
        Comm::srecv(&m1);

        _result = std::stoll(Math::minus(_i == 0 ? m0 : m1, _randK));
    } else {
        std::string sumStr;
        Comm::srecv(&sumStr);
        std::string k0 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand0), _pri
        );
        std::string k1 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand1), _pri
        );
        std::string m0 = Math::add(std::to_string(_m0), k0);
        std::string m1 = Math::add(std::to_string(_m1), k1);
        Comm::ssend(&m0);
        Comm::ssend(&m1);
    }
}

std::string RsaOtExecutor::tag() const {
    return "[RSA OT]";
}

RsaOtExecutor *RsaOtExecutor::reconstruct() {
    return this;
}