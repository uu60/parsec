
#include "../../include/ot/BaseOtBatchOperator.h"

#include <string>
#include <cstring>
#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Math.h"
#include "utils/Crypto.h"
#include "conf/Conf.h"
#include "utils/System.h"

BaseOtBatchOperator::BaseOtBatchOperator(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1,
                                         std::vector<int> *choices, int width, int taskTag, int msgTagOffset)
    : AbstractOtBatchOperator(sender, ms0, ms1, choices, width, taskTag, msgTagOffset) {
}

BaseOtBatchOperator *BaseOtBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start = 0;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (Comm::isServer()) {
        generateAndShareRandoms();
        process();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

void BaseOtBatchOperator::generateAndShareRandoms() {
    // RSA 2048 bits: (2048 >> 3) - 11 = 256 - 11 = 245
    const int len = 245;

    if (_isSender) {
        int size = static_cast<int>(_ms0->size());
        _rand0s.resize(size);
        _rand1s.resize(size);

        std::string combAll;
        for (int i = 0; i < size; ++i) {
            _rand0s[i] = Math::randString(len);
            _rand1s[i] = Math::randString(len);
            combAll += _rand0s[i] + _rand1s[i];
        }
        Comm::serverSend(combAll, buildTag(_currentMsgTag));
    } else {
        int size = static_cast<int>(_choices->size());
        _randKs.resize(size);

        std::string combAll;
        Comm::serverReceive(combAll, buildTag(_currentMsgTag));

        _rand0s.resize(size);
        _rand1s.resize(size);

        for (int i = 0; i < size; ++i) {
            _randKs[i] = Math::randString(len);
            _rand0s[i] = combAll.substr(i * len * 2, len);
            _rand1s[i] = combAll.substr(i * len * 2 + len, len);
        }
    }
}


void BaseOtBatchOperator::process() {
    int size = _isSender ? static_cast<int>(_ms0->size()) : static_cast<int>(_choices->size());

    if (!_isSender) {
        // Receiver: encrypt and send all choices
        std::string allSums;
        for (int i = 0; i < size; ++i) {
            std::string ek = Crypto::rsaEncrypt(_randKs[i], IntermediateDataSupport::_baseOtOtherPub);
            std::string sumStr = Math::add(ek, (*_choices)[i] == 0 ? _rand0s[i] : _rand1s[i]);
            allSums += sumStr;
        }
        Comm::serverSend(allSums, buildTag(_currentMsgTag));

        // Receive all encrypted messages with length prefix
        std::string allM0, allM1;
        Comm::serverReceive(allM0, buildTag(_currentMsgTag));
        Comm::serverReceive(allM1, buildTag(_currentMsgTag));

        _results.resize(size);

        // Process each result based on choice using length-prefix encoding
        size_t m0_offset = 0, m1_offset = 0;
        for (int i = 0; i < size; ++i) {
            // Read length prefix (4 bytes)
            if (m0_offset + 4 > allM0.size() || m1_offset + 4 > allM1.size()) {
                throw std::runtime_error("BaseOtBatchOperator: Invalid message format - insufficient data for length");
            }

            uint32_t len0 = 0, len1 = 0;
            std::memcpy(&len0, allM0.data() + m0_offset, 4);
            std::memcpy(&len1, allM1.data() + m1_offset, 4);
            m0_offset += 4;
            m1_offset += 4;

            // Read the actual data
            if (m0_offset + len0 > allM0.size() || m1_offset + len1 > allM1.size()) {
                throw std::runtime_error("BaseOtBatchOperator: Invalid message format - insufficient data");
            }

            std::string m0 = allM0.substr(m0_offset, len0);
            std::string m1 = allM1.substr(m1_offset, len1);

            m0_offset += len0;
            m1_offset += len1;

            _results[i] = std::stoll(Math::minus((*_choices)[i] == 0 ? m0 : m1, _randKs[i]));
        }
    } else {
        // Sender: receive all sums and decrypt
        std::string allSums;
        Comm::serverReceive(allSums, buildTag(_currentMsgTag));

        std::string allM0, allM1;

        // Process each OT (RSA 2048 bits: 2048 >> 3 = 256 bytes)
        const int sumLen = 256;
        for (int i = 0; i < size; ++i) {
            std::string sumStr = allSums.substr(i * sumLen, sumLen);

            std::string k0 = Crypto::rsaDecrypt(Math::minus(sumStr, _rand0s[i]), IntermediateDataSupport::_baseOtSelfPri);
            std::string k1 = Crypto::rsaDecrypt(Math::minus(sumStr, _rand1s[i]), IntermediateDataSupport::_baseOtSelfPri);

            std::string m0 = Math::add(std::to_string((*_ms0)[i]), k0);
            std::string m1 = Math::add(std::to_string((*_ms1)[i]), k1);

            // Use length-prefix encoding instead of delimiter
            uint32_t len0 = static_cast<uint32_t>(m0.size());
            uint32_t len1 = static_cast<uint32_t>(m1.size());

            allM0.append(reinterpret_cast<const char*>(&len0), 4);
            allM0.append(m0);

            allM1.append(reinterpret_cast<const char*>(&len1), 4);
            allM1.append(m1);
        }

        Comm::serverSend(allM0, buildTag(_currentMsgTag));
        Comm::serverSend(allM1, buildTag(_currentMsgTag));
    }
}

int BaseOtBatchOperator::tagStride() {
    return 1;
}




