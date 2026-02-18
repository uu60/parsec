
#include "ot/BaseOtOperator.h"

#include <string>
#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Math.h"
#include "utils/Crypto.h"

BaseOtOperator::BaseOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset)
    : AbstractOtOperator(sender, m0, m1, choice, l, taskTag, msgTagOffset) {
}

BaseOtOperator *BaseOtOperator::execute() {
    if (Comm::isServer()) {
        generateAndShareRandoms();
        process();
    }
    return this;
}

void BaseOtOperator::generateAndShareRandoms() {
    // RSA 2048 bits: (2048 >> 3) - 11 = 256 - 11 = 245
    const int len = 245;
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


void BaseOtOperator::process() {
    if (!_isSender) {
        std::string ek = Crypto::rsaEncrypt(_randK, IntermediateDataSupport::_baseOtOtherPub);
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
            Math::minus(sumStr, _rand0), IntermediateDataSupport::_baseOtSelfPri
        );
        std::string k1 = Crypto::rsaDecrypt(
            Math::minus(sumStr, _rand1), IntermediateDataSupport::_baseOtSelfPri
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
