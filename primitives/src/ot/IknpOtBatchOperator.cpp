#include "ot/IknpOtBatchOperator.h"

#include "intermediate/IntermediateDataSupport.h"

#include "comm/Comm.h"
#include "conf/Conf.h"
#include "utils/Math.h"

#include <algorithm>
#include <array>
#include <functional>
#include <openssl/evp.h>
#include <vector>

namespace {
// Expand packed sender masks into per-bit messages (0 or -1).
static void expandMasksPacked(const std::vector<int64_t> &packed, std::vector<int64_t> &outMsgs) {
    const int nPacked = static_cast<int>(packed.size());
    outMsgs.clear();
    outMsgs.reserve(static_cast<size_t>(nPacked) * 64);
    for (int i = 0; i < nPacked; ++i) {
        const uint64_t limb = static_cast<uint64_t>(packed[static_cast<size_t>(i)]);
        for (int b = 0; b < 64; ++b) {
            const int bit = static_cast<int>((limb >> b) & 1ULL);
            outMsgs.push_back(bit == 0 ? 0LL : -1LL);
        }
    }
}

// Pack per-bit results (0/-1 masks) back to 0/1 packed bits.
static void packResultsToPackedBits(const std::vector<int64_t> &bitResults, std::vector<int64_t> &outPacked,
                                    int nPacked) {
    outPacked.assign(nPacked, 0);
    int idx = 0;
    for (int i = 0; i < nPacked; ++i) {
        uint64_t limb = 0;
        for (int b = 0; b < 64; ++b) {
            const uint64_t bit = (bitResults[static_cast<size_t>(idx++)] == 0) ? 0ULL : 1ULL;
            limb |= (bit << b);
        }
        outPacked[static_cast<size_t>(i)] = static_cast<int64_t>(limb);
    }
}

inline int64_t choiceToMask(int c) {
    return c == 0 ? 0LL : -1LL;
}

struct U128 {
    uint64_t lo;
    uint64_t hi;
};

class AesCtrPrg {
public:
    explicit AesCtrPrg(uint64_t seed) {
        std::hash<uint64_t> h;
        uint64_t k0 = h(seed ^ 0x9e3779b97f4a7c15ULL);
        uint64_t k1 = h(seed ^ 0xbf58476d1ce4e5b9ULL);
        std::memcpy(_key.data(), &k0, 8);
        std::memcpy(_key.data() + 8, &k1, 8);
        _iv.fill(0);
    }

    void generateBlocks(U128 *out, size_t blocks) const {
        EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
        EVP_EncryptInit_ex(ctx, EVP_aes_128_ctr(), nullptr, _key.data(), _iv.data());

        std::vector<uint8_t> zeros(blocks * 16, 0);
        std::vector<uint8_t> buf(blocks * 16);
        int outLen = 0;
        EVP_EncryptUpdate(ctx, buf.data(), &outLen, zeros.data(), static_cast<int>(zeros.size()));
        EVP_CIPHER_CTX_free(ctx);

        for (size_t i = 0; i < blocks; ++i) {
            uint64_t lo, hi;
            std::memcpy(&lo, buf.data() + i * 16, 8);
            std::memcpy(&hi, buf.data() + i * 16 + 8, 8);
            out[i] = {lo, hi};
        }
    }

private:
    std::array<unsigned char, 16> _key{};
    std::array<unsigned char, 16> _iv{};
};

static void transpose128xNBlocks(const std::array<std::vector<U128>, 128> &rows,
                                 std::vector<U128> &cols,
                                 size_t blocks) {
    cols.assign(blocks * 128, U128{0, 0});

    for (size_t blk = 0; blk < blocks; ++blk) {
        for (int bit = 0; bit < 128; ++bit) {
            uint64_t lo = 0, hi = 0;
            for (int r = 0; r < 128; ++r) {
                const U128 v = rows[r][blk];
                uint64_t word = (bit < 64) ? v.lo : v.hi;
                uint64_t b = (word >> (bit & 63)) & 1ULL;
                if (r < 64) {
                    lo |= (b << r);
                } else {
                    hi |= (b << (r - 64));
                }
            }
            cols[blk * 128 + bit] = {lo, hi};
        }
    }
}
}

IknpOtBatchOperator::IknpOtBatchOperator(int sender,
                                         std::vector<int64_t> *bits0Packed,
                                         std::vector<int64_t> *bits1Packed,
                                         std::vector<int64_t> *choiceBitsPacked,
                                         int taskTag,
                                         int msgTagOffset)
    : AbstractOtBatchOperator(64, taskTag, msgTagOffset) {
    if (Comm::isClient()) {
        return;
    }
    _doBits = true;
    _isSender = Comm::rank() == sender;
    if (_isSender) {
        _ms0 = bits0Packed;
        _ms1 = bits1Packed;
    } else {
        _choiceBitsPacked = choiceBitsPacked;
    }

    _baseOtInitialized = false;
    _extendedOtCount = 0;
}

IknpOtBatchOperator::IknpOtBatchOperator(int sender,
                                         std::vector<int64_t> *ms0,
                                         std::vector<int64_t> *ms1,
                                         std::vector<int> *choices,
                                         int width,
                                         int taskTag,
                                         int msgTagOffset)
    : AbstractOtBatchOperator(sender, ms0, ms1, choices, width, taskTag, msgTagOffset) {
    if (Comm::isClient()) {
        return;
    }
    _doBits = false;
    _choiceBitsPacked = nullptr;
    _baseOtInitialized = false;
    _extendedOtCount = 0;
}

IknpOtBatchOperator *IknpOtBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (!_baseOtInitialized) {
        initializeBaseOt();
        _baseOtInitialized = true;
    }

    if (_doBits) {
        // RandOt for-bits semantics: each packed limb carries 64 independent 1-bit choices.
        // To match outputs exactly, we execute 64*m standard IKNP OTs (one per bit), then
        // pack the per-bit results back into 64-bit limbs.
        const bool isSender = _isSender;
        const int nPacked = isSender ? static_cast<int>(_ms0 ? _ms0->size() : 0)
                                     : static_cast<int>(_choiceBitsPacked ? _choiceBitsPacked->size() : 0);

        std::vector<int64_t> ms0Bits;
        std::vector<int64_t> ms1Bits;
        std::vector<int> choicesBits;

        if (isSender) {
            if (!_ms0 || !_ms1) {
                throw std::invalid_argument("IknpOtBatchOperator(bits) sender ms0/ms1 is null");
            }
            if (_ms0->size() != _ms1->size()) {
                throw std::invalid_argument("IknpOtBatchOperator(bits) sender ms0/ms1 size mismatch");
            }
            expandMasksPacked(*_ms0, ms0Bits);
            expandMasksPacked(*_ms1, ms1Bits);
        } else {
            if (!_choiceBitsPacked) {
                throw std::invalid_argument("IknpOtBatchOperator(bits) receiver choiceBitsPacked is null");
            }

            // choicesPacked can be either 0/1 packed bits or 0/-1 packed masks.
            // In both cases, bit i is simply ((uint64_t)limb >> i) & 1.
            choicesBits.clear();
            choicesBits.reserve(static_cast<size_t>(nPacked) * 64);
            for (int i = 0; i < nPacked; ++i) {
                const uint64_t limb = static_cast<uint64_t>((*_choiceBitsPacked)[static_cast<size_t>(i)]);
                for (int b = 0; b < 64; ++b) {
                    choicesBits.push_back(static_cast<int>((limb >> b) & 1ULL));
                }
            }
        }

        // Temporarily swap pointers to reuse existing senderExtend/receiverExtend.
        auto *savedMs0 = _ms0;
        auto *savedMs1 = _ms1;
        auto *savedChoices = _choices;

        if (isSender) {
            _ms0 = &ms0Bits;
            _ms1 = &ms1Bits;
            _choices = nullptr;
        } else {
            _ms0 = nullptr;
            _ms1 = nullptr;
            _choices = &choicesBits;
        }

        if (_isSender) {
            senderExtend();
        } else {
            receiverExtend();
        }

        // restore
        _ms0 = savedMs0;
        _ms1 = savedMs1;
        _choices = savedChoices;

        if (!isSender) {
            std::vector<int64_t> packedRes;
            packResultsToPackedBits(_results, packedRes, nPacked);
            _results = std::move(packedRes);
        }
    } else {
        if (_isSender) {
            senderExtend();
        } else {
            receiverExtend();
        }
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int IknpOtBatchOperator::tagStride() {
    return 2;
}

void IknpOtBatchOperator::initializeBaseOt() {
    _senderSeeds.clear();
    _senderSeeds.resize(SECURITY_PARAM);

    if (IntermediateDataSupport::_iknpBaseSeeds.empty()) {
        IntermediateDataSupport::prepareIknpSeeds();
    }

    const uint64_t expansionId = IntermediateDataSupport::nextIknpExpansionId();
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        const auto &base = IntermediateDataSupport::_iknpBaseSeeds[i];
        int64_t s0 = static_cast<int64_t>(static_cast<uint64_t>(base[0]) ^ expansionId ^ (static_cast<uint64_t>(i) << 32));
        int64_t s1 = static_cast<int64_t>(static_cast<uint64_t>(base[1]) ^ expansionId ^ (static_cast<uint64_t>(i) << 32));
        _senderSeeds[i] = {hashFunction(i, s0), hashFunction(i, s1)};
    }
}

void IknpOtBatchOperator::senderExtend() {
    const int m = static_cast<int>(_ms0 ? _ms0->size() : 0);
    if (m <= 0) {
        return;
    }
    _extendedOtCount = m;

    const size_t blocks = (static_cast<size_t>(m) + 127) / 128;

    // Receive u from receiver.
    std::vector<int64_t> uRaw;
    auto rU = Comm::serverReceiveAsync(uRaw, static_cast<int>(SECURITY_PARAM * blocks * 2), 64, buildTag(_currentMsgTag));
    Comm::wait(rU);

    std::array<std::vector<U128>, 128> rowsU;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        rowsU[i].resize(blocks);
        for (size_t blk = 0; blk < blocks; ++blk) {
            const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
            rowsU[i][blk] = {static_cast<uint64_t>(uRaw[off]), static_cast<uint64_t>(uRaw[off + 1])};
        }
    }

    // Generate q rows using sender seeds (same as normal IKNP).
    std::array<std::vector<U128>, 128> qRows;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        qRows[i].resize(blocks);
        AesCtrPrg gen(static_cast<uint64_t>(_senderSeeds[i][0]));
        gen.generateBlocks(qRows[i].data(), blocks);
    }

    std::vector<U128> qCols0;
    transpose128xNBlocks(qRows, qCols0, blocks);

    std::vector<U128> uCols;
    transpose128xNBlocks(rowsU, uCols, blocks);

    // Send masked payloads per packed limb.
    std::vector<int64_t> sendBuf(m * 2);
    for (int j = 0; j < m; ++j) {
        const U128 q0 = qCols0[static_cast<size_t>(j)];
        const U128 u = uCols[static_cast<size_t>(j)];
        const U128 q1 = {q0.lo ^ u.lo, q0.hi ^ u.hi};

        const int64_t pad0 = hashFunction(j, static_cast<int64_t>(q0.lo ^ q0.hi));
        const int64_t pad1 = hashFunction(j, static_cast<int64_t>(q1.lo ^ q1.hi));

        const int64_t m0 = (*_ms0)[j];
        const int64_t m1 = (*_ms1)[j];

        sendBuf[j * 2] = Math::ring(m0 ^ pad0, 64);
        sendBuf[j * 2 + 1] = Math::ring(m1 ^ pad1, 64);
    }

    auto rSend = Comm::serverSendAsync(sendBuf, 64, buildTag(_currentMsgTag + 1));
    Comm::wait(rSend);

    _currentMsgTag += tagStride();
}

void IknpOtBatchOperator::receiverExtend() {
    const int m = _choices ? static_cast<int>(_choices->size()) : 0;
    if (m <= 0) {
        return;
    }
    _extendedOtCount = m;

    const size_t blocks = (static_cast<size_t>(m) + 127) / 128;

    std::vector<int64_t> rMask(m);
    for (int j = 0; j < m; ++j) {
        rMask[j] = choiceToMask((*_choices)[j]);
    }

    std::vector<U128> rPacked(blocks, U128{0, 0});
    for (int j = 0; j < m; ++j) {
        const size_t blk = static_cast<size_t>(j) / 128;
        const int bit = j % 128;
        const uint64_t b = (rMask[j] == 0) ? 0ULL : 1ULL;
        if (bit < 64) {
            rPacked[blk].lo |= (b << bit);
        } else {
            rPacked[blk].hi |= (b << (bit - 64));
        }
    }

    std::array<std::vector<U128>, 128> tRows0;
    std::array<std::vector<U128>, 128> tRows1;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        tRows0[i].resize(blocks);
        tRows1[i].resize(blocks);
        AesCtrPrg g0(static_cast<uint64_t>(_senderSeeds[i][0]));
        AesCtrPrg g1(static_cast<uint64_t>(_senderSeeds[i][1]));
        g0.generateBlocks(tRows0[i].data(), blocks);
        g1.generateBlocks(tRows1[i].data(), blocks);
    }

    std::vector<int64_t> uRaw;
    uRaw.resize(static_cast<size_t>(SECURITY_PARAM) * blocks * 2);
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        for (size_t blk = 0; blk < blocks; ++blk) {
            U128 u = {tRows0[i][blk].lo ^ tRows1[i][blk].lo ^ rPacked[blk].lo,
                      tRows0[i][blk].hi ^ tRows1[i][blk].hi ^ rPacked[blk].hi};
            const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
            uRaw[off] = static_cast<int64_t>(u.lo);
            uRaw[off + 1] = static_cast<int64_t>(u.hi);
        }
    }

    auto rSendU = Comm::serverSendAsync(uRaw, 64, buildTag(_currentMsgTag));

    std::vector<int64_t> recvBuf;
    auto rRecv = Comm::serverReceiveAsync(recvBuf, m * 2, 64, buildTag(_currentMsgTag + 1));

    Comm::wait(rSendU);
    Comm::wait(rRecv);

    std::vector<U128> tCols0;
    transpose128xNBlocks(tRows0, tCols0, blocks);

    std::vector<U128> uCols;
    {
        std::array<std::vector<U128>, 128> rowsU;
        for (int i = 0; i < SECURITY_PARAM; ++i) {
            rowsU[i].resize(blocks);
            for (size_t blk = 0; blk < blocks; ++blk) {
                const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
                rowsU[i][blk] = {static_cast<uint64_t>(uRaw[off]), static_cast<uint64_t>(uRaw[off + 1])};
            }
        }
        transpose128xNBlocks(rowsU, uCols, blocks);
    }

    _results.resize(m);
    for (int j = 0; j < m; ++j) {
        const int c = (*_choices)[j];
        const U128 t0 = tCols0[static_cast<size_t>(j)];
        const U128 u = uCols[static_cast<size_t>(j)];
        const U128 q = (c == 0) ? t0 : U128{t0.lo ^ u.lo, t0.hi ^ u.hi};
        const int64_t pad = hashFunction(j, static_cast<int64_t>(q.lo ^ q.hi));

        const int64_t masked = recvBuf[j * 2 + c];
        _results[j] = Math::ring(masked ^ pad, _width);
    }

    _currentMsgTag += tagStride();
}

int64_t IknpOtBatchOperator::hashFunction(int index, int64_t value) {
    std::hash<int64_t> h;
    int64_t x = value ^ (static_cast<int64_t>(index) * 0x9e3779b97f4a7c15ULL);
    return static_cast<int64_t>(h(x) ^ h(value) ^ h(static_cast<int64_t>(index)));
}
