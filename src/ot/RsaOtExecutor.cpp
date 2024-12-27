//
// Created by 杜建璋 on 2024/7/15.
//

#include "ot/RsaOtExecutor.h"

#include "comm/IComm.h"
#include "utils/Math.h"
#include "utils/Crypto.h"
#include "utils/Log.h"

RsaOtExecutor::RsaOtExecutor(int sender, int64_t m0, int64_t m1, int i, int l, int16_t objTag, int16_t msgTagOffset)
    : RsaOtExecutor(2048, sender, m0, m1, i, l, objTag, msgTagOffset) {
}

RsaOtExecutor::RsaOtExecutor(int bits, int sender, int64_t m0, int64_t m1, int i, int l, int16_t objTag,
                             int16_t msgTagOffset) : AbstractSecureExecutor(l, objTag, msgTagOffset) {
    _bits = bits;
    _isSender = sender == IComm::impl->rank();
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

void RsaOtExecutor::generateAndShareRandoms() {
    // 11 for PKCS#1 v1.5 padding
    int len = (_bits >> 3) - 11;
    auto msgTags = nextMsgTags(2);
    if (_isSender) {
        _rand0 = Math::randString(len);
        _rand1 = Math::randString(len);

        auto f0 = System::_threadPool.push([this, msgTags] (int _) {
            IComm::impl->serverSend(&_rand0, buildTag(msgTags[0]));
        });
        auto f1 = System::_threadPool.push([this, msgTags] (int _) {
            IComm::impl->serverSend(&_rand1, buildTag(msgTags[1]));
        });
        f0.wait();
        f1.wait();
    } else {
        _randK = Math::randString(len);
        IComm::impl->serverReceive(&_rand0, buildTag(msgTags[0]));
        IComm::impl->serverReceive(&_rand1, buildTag(msgTags[1]));
    }
}

void RsaOtExecutor::generateAndShareRsaKeys() {
    if (_isSender) {
        bool newKey = Crypto::generateRsaKeys(_bits);
        _pub = Crypto::_selfPubs[_bits];
        _pri = Crypto::_selfPris[_bits];
        if (newKey) {
            IComm::impl->serverSend(&_pub, buildTag(_currentMsgTag++));
        }
    } else {
        // receiver
        if (Crypto::_otherPubs.count(_bits) > 0) {
            _pub = Crypto::_otherPubs[_bits];
        } else {
            IComm::impl->serverReceive(&_pub, buildTag(_currentMsgTag++));
            Crypto::_otherPubs[_bits] = _pub;
        }
    }
}

void RsaOtExecutor::process() {
    auto msgTags = nextMsgTags(3);
    if (!_isSender) {
        std::string ek = Crypto::rsaEncrypt(_randK, _pub);
        std::string sumStr = Math::add(ek, _i == 0 ? _rand0 : _rand1);
        IComm::impl->serverSend(&sumStr, buildTag(msgTags[0]));

        std::string m0, m1;
        IComm::impl->serverReceive(&m0, buildTag(msgTags[1]));
        IComm::impl->serverReceive(&m1, buildTag(msgTags[2]));

        _result = std::stoll(Math::minus(_i == 0 ? m0 : m1, _randK));
    } else {
        std::string sumStr;
        IComm::impl->serverReceive(&sumStr, buildTag(msgTags[0]));
        std::string k0 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand0), _pri
        );
        std::string k1 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand1), _pri
        );
        std::string m0 = Math::add(std::to_string(_m0), k0);
        std::string m1 = Math::add(std::to_string(_m1), k1);

        auto f0 = System::_threadPool.push([m0, msgTags, this] (int _) {
            IComm::impl->serverSend(&m0, buildTag(msgTags[1]));
        });
        auto f1 = System::_threadPool.push([m1, msgTags, this] (int _) {
            IComm::impl->serverSend(&m1, buildTag(msgTags[2]));
        });
        f0.wait();
        f1.wait();
    }
}

std::string RsaOtExecutor::className() const {
    return "RsaOtExecutor";
}

RsaOtExecutor *RsaOtExecutor::reconstruct(int clientRank) {
    return this;
}

int16_t RsaOtExecutor::neededMsgTags() {
    return 6;
}
