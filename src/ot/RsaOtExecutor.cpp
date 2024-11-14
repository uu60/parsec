//
// Created by 杜建璋 on 2024/7/15.
//

#include "ot/RsaOtExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"
#include "utils/Crypto.h"

template<typename T>
RsaOtExecutor<T>::RsaOtExecutor(int sender, T m0, T m1, int i)
    : RsaOtExecutor(2048, sender, m0, m1, i) {
}

template<typename T>
RsaOtExecutor<T>::RsaOtExecutor(int bits, int sender, T m0, T m1, int i) {
    _bits = bits;
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _m0 = m0;
        _m1 = m1;
    } else {
        _i = i;
    }
}

template<typename T>
RsaOtExecutor<T> *RsaOtExecutor<T>::execute(bool dummy) {
    // preparation
    generateAndShareRandoms();
    generateAndShareRsaKeys();

    // process
    process();
    return this;
}

template<typename T>
void RsaOtExecutor<T>::generateAndShareRsaKeys() {
    if (_isSender) {
        bool newKey = Crypto::generateRsaKeys(_bits);
        this->_pub = Crypto::_selfPubs[_bits];
        this->_pri = Crypto::_selfPris[_bits];
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

template<typename T>
void RsaOtExecutor<T>::generateAndShareRandoms() {
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

template<typename T>
void RsaOtExecutor<T>::process() {
    if (!_isSender) {
        std::string ek = Crypto::rsaEncrypt(_randK, _pub);
        std::string sumStr = Math::add(ek, _i == 0 ? _rand0 : _rand1);
        Comm::ssend(&sumStr);

        std::string m0, m1;
        Comm::srecv(&m0);
        Comm::srecv(&m1);

        this->_result = std::stoll(Math::minus(_i == 0 ? m0 : m1, _randK));
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

template<typename T>
std::string RsaOtExecutor<T>::tag() const {
    return "[RSA OT]";
}

template<typename T>
RsaOtExecutor<T> *RsaOtExecutor<T>::reconstruct() {
    return this;
}

template class RsaOtExecutor<bool>;
template class RsaOtExecutor<int8_t>;
template class RsaOtExecutor<int16_t>;
template class RsaOtExecutor<int32_t>;
template class RsaOtExecutor<int64_t>;
