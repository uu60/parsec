//
// Created by 杜建璋 on 2024/7/15.
//

#include "ot/BaseOtOperator.h"

#include "comm/Comm.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"
#include "utils/Crypto.h"
#include "utils/Log.h"

BaseOtOperator::BaseOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset)
    : BaseOtOperator(2048, sender, m0, m1, choice, l, taskTag, msgTagOffset) {
}

BaseOtOperator::BaseOtOperator(int bits, int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag,
                               int msgTagOffset) : AbstractOtOperator(sender, m0, m1, choice, l, taskTag,
                                                                      msgTagOffset) {
    _bits = bits;
}

BaseOtOperator *BaseOtOperator::execute() {
    if (Comm::isServer()) {
        // preparation
        generateAndShareRandoms();
        generateAndShareRsaKeys();

        // process
        process();
    }
    return this;
}

void BaseOtOperator::generateAndShareRandoms() {
    // 11 for PKCS#1 v1.5 padding
    int len = (_bits >> 3) - 11;
    if (_isSender) {
        _rand0 = Math::randString(len);
        _rand1 = Math::randString(len);

        std::string comb = _rand0 + _rand1;
        Comm::serverSend(comb, buildTag(_currentMsgTag));
    } else {
        _randK = Math::randString(len);
        std::string comb;
        Comm::serverReceive(comb, buildTag(_currentMsgTag));
        _rand0 = comb.substr(0, len);
        _rand1 = comb.substr(len, len);
    }
}

void BaseOtOperator::generateAndShareRsaKeys() {
    if (_isSender) {
        bool newKey = Crypto::generateRsaKeys(_bits);
        _pub = Crypto::_selfPubs[_bits];
        _pri = Crypto::_selfPris[_bits];
        if (newKey) {
            Comm::serverSend(_pub, buildTag(_currentMsgTag));
        }
    } else {
        // receiver
        if (Crypto::_otherPubs.count(_bits) > 0) {
            _pub = Crypto::_otherPubs[_bits];
        } else {
            Comm::serverReceive(_pub, buildTag(_currentMsgTag));
            Crypto::_otherPubs[_bits] = _pub;
        }
    }
}

void BaseOtOperator::process() {
    if (!_isSender) {
        std::string ek = Crypto::rsaEncrypt(_randK, _pub);
        std::string sumStr = Math::add(ek, _choice == 0 ? _rand0 : _rand1);
        Comm::serverSend(sumStr, buildTag(_currentMsgTag));

        std::string m0, m1;
        Comm::serverReceive(m0, buildTag(_currentMsgTag));
        Comm::serverReceive(m1, buildTag(_currentMsgTag));

        _result = std::stoll(Math::minus(_choice == 0 ? m0 : m1, _randK));
    } else {
        std::string sumStr;
        Comm::serverReceive(sumStr, buildTag(_currentMsgTag));
        std::string k0 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand0), _pri
        );
        std::string k1 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand1), _pri
        );
        std::string m0 = Math::add(std::to_string(_m0), k0);
        std::string m1 = Math::add(std::to_string(_m1), k1);

        Comm::serverSend(m0, buildTag(_currentMsgTag));
        Comm::serverSend(m1, buildTag(_currentMsgTag));
    }
}

int BaseOtOperator::tagStride() {
    return 1;
}
