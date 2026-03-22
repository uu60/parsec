#include "../../include/ot/BaseOtBatchOperator.h"

#include <string>
#include <cstring>
#include <stdexcept>
#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Math.h"
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
    if (_isSender) {
        RsaKeyContext rsaCtx = Math::loadRsaPrivateKey(IntermediateDataSupport::_baseOtSelfPri);
        int nbytes = rsaCtx.nbytes;
        int size = static_cast<int>(_ms0->size());

        _rand0s.resize(size);
        _rand1s.resize(size);

        std::string payload;

        for (int i = 0; i < size; ++i) {
            _rand0s[i] = Math::sampleZStarN(rsaCtx);
            _rand1s[i] = Math::sampleZStarN(rsaCtx);

            uint32_t len = static_cast<uint32_t>(nbytes);
            payload.append(reinterpret_cast<const char*>(&len), 4);
            payload.append(_rand0s[i]);
            payload.append(reinterpret_cast<const char*>(&len), 4);
            payload.append(_rand1s[i]);
        }

        Comm::serverSend(payload, buildTag(_currentMsgTag));

    } else {
        std::string payload;
        Comm::serverReceive(payload, buildTag(_currentMsgTag));

        RsaKeyContext rsaCtx = Math::loadRsaPublicKey(IntermediateDataSupport::_baseOtOtherPub);
        int nbytes = rsaCtx.nbytes;
        int size = static_cast<int>(_choices->size());

        _rand0s.resize(size);
        _rand1s.resize(size);
        _randKs.resize(size);

        size_t offset = 0;
        for (int i = 0; i < size; ++i) {
            if (offset + 4 > payload.size()) {
                throw std::runtime_error("generateAndShareRandoms: bad payload - insufficient data for x0 length");
            }
            uint32_t len0 = 0;
            std::memcpy(&len0, payload.data() + offset, 4);
            offset += 4;

            if (static_cast<int>(len0) != nbytes) {
                throw std::runtime_error("generateAndShareRandoms: bad payload - x0 length mismatch");
            }
            if (offset + len0 > payload.size()) {
                throw std::runtime_error("generateAndShareRandoms: bad payload - insufficient data for x0");
            }
            _rand0s[i] = payload.substr(offset, len0);
            offset += len0;

            if (offset + 4 > payload.size()) {
                throw std::runtime_error("generateAndShareRandoms: bad payload - insufficient data for x1 length");
            }
            uint32_t len1 = 0;
            std::memcpy(&len1, payload.data() + offset, 4);
            offset += 4;

            if (static_cast<int>(len1) != nbytes) {
                throw std::runtime_error("generateAndShareRandoms: bad payload - x1 length mismatch");
            }
            if (offset + len1 > payload.size()) {
                throw std::runtime_error("generateAndShareRandoms: bad payload - insufficient data for x1");
            }
            _rand1s[i] = payload.substr(offset, len1);
            offset += len1;

            _randKs[i] = Math::sampleZStarN(rsaCtx);
        }
    }
}


void BaseOtBatchOperator::process() {
    int size = _isSender ? static_cast<int>(_ms0->size()) : static_cast<int>(_choices->size());

    if (!_isSender) {
        RsaKeyContext rsaCtx = Math::loadRsaPublicKey(IntermediateDataSupport::_baseOtOtherPub);
        int nbytes = rsaCtx.nbytes;
        std::string e_bytes = Math::getExponentE(rsaCtx);

        std::string allV;

        for (int i = 0; i < size; ++i) {
            int b = (*_choices)[i] & 1;

            const std::string& xb_bytes = (b == 0) ? _rand0s[i] : _rand1s[i];
            const std::string& k_bytes = _randKs[i];

            std::string ke_bytes = Math::modExp(k_bytes, e_bytes, rsaCtx);

            std::string v_bytes = Math::modMul(ke_bytes, xb_bytes, rsaCtx);

            uint32_t len = static_cast<uint32_t>(nbytes);
            allV.append(reinterpret_cast<const char*>(&len), 4);
            allV.append(v_bytes);
        }

        Comm::serverSend(allV, buildTag(_currentMsgTag));
        std::string allM0, allM1;
        Comm::serverReceive(allM0, buildTag(_currentMsgTag));
        Comm::serverReceive(allM1, buildTag(_currentMsgTag));

        _results.resize(size);

        size_t m0_offset = 0, m1_offset = 0;
        for (int i = 0; i < size; ++i) {
            int b = (*_choices)[i] & 1;

            if (m0_offset + 4 > allM0.size()) {
                throw std::runtime_error("process: bad allM0 - insufficient data for length");
            }
            uint32_t len0 = 0;
            std::memcpy(&len0, allM0.data() + m0_offset, 4);
            m0_offset += 4;

            if (len0 != 8) {
                throw std::runtime_error("process: bad allM0 - expected length 8");
            }
            if (m0_offset + len0 > allM0.size()) {
                throw std::runtime_error("process: bad allM0 - insufficient data for ciphertext");
            }
            int64_t c0 = 0;
            std::memcpy(&c0, allM0.data() + m0_offset, 8);
            m0_offset += 8;

            if (m1_offset + 4 > allM1.size()) {
                throw std::runtime_error("process: bad allM1 - insufficient data for length");
            }
            uint32_t len1 = 0;
            std::memcpy(&len1, allM1.data() + m1_offset, 4);
            m1_offset += 4;

            if (len1 != 8) {
                throw std::runtime_error("process: bad allM1 - expected length 8");
            }
            if (m1_offset + len1 > allM1.size()) {
                throw std::runtime_error("process: bad allM1 - insufficient data for ciphertext");
            }
            int64_t c1 = 0;
            std::memcpy(&c1, allM1.data() + m1_offset, 8);
            m1_offset += 8;

            int64_t pad = static_cast<int64_t>(Math::kdfSha256To8Bytes(_randKs[i]));

            int64_t cb = (b == 0) ? c0 : c1;
            _results[i] = cb ^ pad;
        }

    } else {
        RsaKeyContext rsaCtx = Math::loadRsaPrivateKey(IntermediateDataSupport::_baseOtSelfPri);
        int nbytes = rsaCtx.nbytes;
        std::string d_bytes = Math::getExponentD(rsaCtx);

        std::string allV;
        Comm::serverReceive(allV, buildTag(_currentMsgTag));

        std::string allM0, allM1;

        size_t v_offset = 0;
        for (int i = 0; i < size; ++i) {
            if (v_offset + 4 > allV.size()) {
                throw std::runtime_error("process: bad allV - insufficient data for v length");
            }
            uint32_t vlen = 0;
            std::memcpy(&vlen, allV.data() + v_offset, 4);
            v_offset += 4;

            if (static_cast<int>(vlen) != nbytes) {
                throw std::runtime_error("process: bad allV - v length mismatch");
            }
            if (v_offset + vlen > allV.size()) {
                throw std::runtime_error("process: bad allV - insufficient data for v");
            }
            std::string v_bytes = allV.substr(v_offset, vlen);
            v_offset += vlen;

            std::string y_bytes = Math::rsaDecrypt(v_bytes, rsaCtx);

            std::string x0d_bytes = Math::modExp(_rand0s[i], d_bytes, rsaCtx);
            std::string x1d_bytes = Math::modExp(_rand1s[i], d_bytes, rsaCtx);

            std::string inv0_bytes, inv1_bytes;
            try {
                inv0_bytes = Math::modInverse(x0d_bytes, rsaCtx);
            } catch (const std::exception& ex) {
                throw std::runtime_error("process: modInverse failed for x0d at i=" + std::to_string(i));
            }
            try {
                inv1_bytes = Math::modInverse(x1d_bytes, rsaCtx);
            } catch (const std::exception& ex) {
                throw std::runtime_error("process: modInverse failed for x1d at i=" + std::to_string(i));
            }

            std::string k0_bytes = Math::modMul(y_bytes, inv0_bytes, rsaCtx);
            std::string k1_bytes = Math::modMul(y_bytes, inv1_bytes, rsaCtx);

            int64_t pad0 = static_cast<int64_t>(Math::kdfSha256To8Bytes(k0_bytes));
            int64_t pad1 = static_cast<int64_t>(Math::kdfSha256To8Bytes(k1_bytes));

            int64_t c0 = (*_ms0)[i] ^ pad0;
            int64_t c1 = (*_ms1)[i] ^ pad1;

            uint32_t len = 8;
            allM0.append(reinterpret_cast<const char*>(&len), 4);
            allM0.append(reinterpret_cast<const char*>(&c0), 8);

            allM1.append(reinterpret_cast<const char*>(&len), 4);
            allM1.append(reinterpret_cast<const char*>(&c1), 8);
        }

        Comm::serverSend(allM0, buildTag(_currentMsgTag));
        Comm::serverSend(allM1, buildTag(_currentMsgTag));
    }
}

int BaseOtBatchOperator::tagStride() {
    return 1;
}

