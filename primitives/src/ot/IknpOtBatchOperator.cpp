#include "ot/IknpOtBatchOperator.h"

#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <array>
#include <algorithm>
#include <cstring>
#include <functional>
#include <openssl/evp.h>
#include <stdexcept>
#include <vector>

namespace {
    struct U128 {
        uint64_t lo;
        uint64_t hi;
    };

    static inline U128 xorU128(const U128 &a, const U128 &b) {
        return {a.lo ^ b.lo, a.hi ^ b.hi};
    }

    // Fast 128x128 bit-matrix transpose (bit-sliced) using 64-bit swap layers.
    // Input: 128 rows, each is a 128-bit word (lo,hi). Output: 128 rows of transposed.
    static inline void transpose128x128_inplace(U128 v[128]) {
        uint64_t x0[128];
        uint64_t x1[128];
        for (int i = 0; i < 128; ++i) {
            x0[i] = v[i].lo;
            x1[i] = v[i].hi;
        }

        // Transpose the 128x128 matrix represented as two 128x64 halves.
        // We do staged swaps within 64-bit words, then swap across the 64-bit boundary.
        auto swap_layer = [](uint64_t *a, uint64_t *b, uint64_t mask, int shift) {
            for (int i = 0; i < 128; ++i) {
                const uint64_t ta = (a[i] >> shift) & mask;
                const uint64_t tb = (b[i] & mask);
                a[i] = (a[i] & ~(mask << shift)) | (tb << shift);
                b[i] = (b[i] & ~mask) | ta;
            }
        };

        // In-word transpose for each 64-bit slice via butterfly network across rows.
        // We implement the classic Eklundh style using masks and shifts across the row index.
        // First operate on x0 and x1 separately.
        auto transpose64 = [](uint64_t x[128]) {
            // swap bit pairs across rows
            static constexpr uint64_t m1 = 0x5555555555555555ULL;
            static constexpr uint64_t m2 = 0x3333333333333333ULL;
            static constexpr uint64_t m4 = 0x0f0f0f0f0f0f0f0fULL;
            static constexpr uint64_t m8 = 0x00ff00ff00ff00ffULL;
            static constexpr uint64_t m16 = 0x0000ffff0000ffffULL;
            static constexpr uint64_t m32 = 0x00000000ffffffffULL;

            auto swap_rows = [&](int shift, uint64_t mask) {
                for (int i = 0; i < 128; i += 2 * shift) {
                    for (int j = 0; j < shift; ++j) {
                        uint64_t a = x[i + j];
                        uint64_t b = x[i + j + shift];
                        uint64_t t = ((a >> shift) ^ b) & mask;
                        b ^= t;
                        a ^= (t << shift);
                        x[i + j] = a;
                        x[i + j + shift] = b;
                    }
                }
            };

            swap_rows(1, m1);
            swap_rows(2, m2);
            swap_rows(4, m4);
            swap_rows(8, m8);
            swap_rows(16, m16);
            swap_rows(32, m32);
        };

        // This produces transpose within each 64-bit half, but we still need to handle cross-half bits.
        // We transpose each half, then swap corresponding 64-bit columns between halves.
        transpose64(x0);
        transpose64(x1);

        for (int i = 0; i < 64; ++i) {
            std::swap(x0[i + 64], x1[i]);
        }

        for (int i = 0; i < 128; ++i) {
            v[i] = {x0[i], x1[i]};
        }
    }

    // Transpose 128 rows (each row is `blocks` U128) into `cols` (blocks*128 entries).
    // Fast path: transpose each 128x128 tile independently.
    static void transpose128xNBlocks(const std::array<std::vector<U128>, 128> &rows,
                                     std::vector<U128> &cols,
                                     size_t blocks) {
        cols.assign(blocks * 128, U128{0, 0});

        U128 tile[128];
        for (size_t blk = 0; blk < blocks; ++blk) {
            for (int r = 0; r < 128; ++r) {
                tile[r] = rows[r][blk];
            }
            transpose128x128_inplace(tile);
            for (int bit = 0; bit < 128; ++bit) {
                cols[blk * 128 + static_cast<size_t>(bit)] = tile[bit];
            }
        }
    }

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

        // Reuse EVP context and avoid per-call allocations.
        void generateBlocks(U128 *out, size_t blocks) const {
            if (blocks == 0) return;

            EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
            if (!ctx) {
                throw std::runtime_error("EVP_CIPHER_CTX_new failed");
            }
            if (EVP_EncryptInit_ex(ctx, EVP_aes_128_ctr(), nullptr, _key.data(), _iv.data()) != 1) {
                EVP_CIPHER_CTX_free(ctx);
                throw std::runtime_error("EVP_EncryptInit_ex failed");
            }

            std::vector<unsigned char> buf(blocks * 16);
            int outLen = 0;
            // Encrypt an all-zero stream: input can alias a static zero buffer by passing nullptr? OpenSSL doesn't allow.
            // So create a small zero chunk and stream.
            static constexpr size_t CHUNK = 1 << 12; // 4KB
            std::array<unsigned char, CHUNK> zeros{};

            size_t written = 0;
            while (written < blocks * 16) {
                const size_t n = std::min(CHUNK, blocks * 16 - written);
                int l = 0;
                if (EVP_EncryptUpdate(ctx, buf.data() + written, &l, zeros.data(), static_cast<int>(n)) != 1) {
                    EVP_CIPHER_CTX_free(ctx);
                    throw std::runtime_error("EVP_EncryptUpdate failed");
                }
                written += static_cast<size_t>(l);
            }

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


    static void expandMasksPacked64(const std::vector<int64_t> &packed, std::vector<int64_t> &outMsgs) {
        outMsgs.clear();
        outMsgs.reserve(packed.size() * 64);
        for (size_t i = 0; i < packed.size(); ++i) {
            const uint64_t limb = static_cast<uint64_t>(packed[i]);
            for (int b = 0; b < 64; ++b) {
                const int bit = static_cast<int>((limb >> b) & 1ULL);
                outMsgs.push_back(bit == 0 ? 0LL : -1LL);
            }
        }
    }

    static void expandChoicesPacked64(const std::vector<int64_t> &packed, std::vector<int> &outChoices) {
        outChoices.clear();
        outChoices.reserve(packed.size() * 64);
        for (size_t i = 0; i < packed.size(); ++i) {
            const uint64_t limb = static_cast<uint64_t>(packed[i]);
            for (int b = 0; b < 64; ++b) {
                outChoices.push_back(static_cast<int>((limb >> b) & 1ULL));
            }
        }
    }

    static void packResultsToBits64(const std::vector<int64_t> &bitResults, std::vector<int64_t> &outPacked,
                                    size_t nPacked) {
        outPacked.assign(nPacked, 0);
        size_t idx = 0;
        for (size_t i = 0; i < nPacked; ++i) {
            uint64_t limb = 0;
            for (int b = 0; b < 64; ++b) {
                const uint64_t bit = (bitResults[idx++] == 0) ? 0ULL : 1ULL;
                limb |= (bit << b);
            }
            outPacked[i] = static_cast<int64_t>(limb);
        }
    }
} // namespace

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
    _isSender = (Comm::rank() == sender);
    if (_isSender) {
        _ms0 = bits0Packed;
        _ms1 = bits1Packed;
    } else {
        _choiceBitsPacked = choiceBitsPacked;
    }
    _baseReady = false;
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
    _baseReady = false;
}

int IknpOtBatchOperator::tagStride() {
    return 2;
}

void IknpOtBatchOperator::senderExtendForBits() {
    const size_t nPacked = _ms0->size(); // 每个是 64-bit
    if (nPacked == 0) return;

    const size_t m = nPacked * 64;
    const size_t blocks = (m + 127) / 128;

    // 1) 接收 u（128 × blocks × 2 limbs）
    std::vector<int64_t> uRaw;
    auto rU = Comm::serverReceiveAsync(
        uRaw,
        SECURITY_PARAM * blocks * 2,
        64,
        buildTag(_currentMsgTag)
    );
    Comm::wait(rU);

    // rowsU[i][blk]
    std::array<std::vector<U128>, 128> rowsU;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        rowsU[i].resize(blocks);
        for (size_t blk = 0; blk < blocks; ++blk) {
            size_t off = (i * blocks + blk) * 2;
            rowsU[i][blk] = {
                (uint64_t) uRaw[off],
                (uint64_t) uRaw[off + 1]
            };
        }
    }

    // 2) PRG 生成 q0 rows
    std::array<std::vector<U128>, 128> qRows;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        qRows[i].resize(blocks);
        AesCtrPrg gen((uint64_t) _senderSeeds[i][0]);
        gen.generateBlocks(qRows[i].data(), blocks);
    }

    // 3) transpose → column OT
    std::vector<U128> qCols0, uCols;
    transpose128xNBlocks(qRows, qCols0, blocks);
    transpose128xNBlocks(rowsU, uCols, blocks);

    // 4) bit-sliced mask
    std::vector<int64_t> sendBuf(nPacked * 2);

    for (size_t p = 0; p < nPacked; ++p) {
        const size_t j = p * 64;

        const U128 q0 = qCols0[j];
        const U128 u = uCols[j];
        const U128 q1 = {q0.lo ^ u.lo, q0.hi ^ u.hi};

        int64_t pad0 = hash64(j, (int64_t) (q0.lo ^ q0.hi));
        int64_t pad1 = hash64(j, (int64_t) (q1.lo ^ q1.hi));

        sendBuf[p * 2] = (*_ms0)[p] ^ pad0;
        sendBuf[p * 2 + 1] = (*_ms1)[p] ^ pad1;
    }

    auto rSend = Comm::serverSendAsync(
        sendBuf,
        64,
        buildTag(_currentMsgTag + 1)
    );
    Comm::wait(rSend);

    _currentMsgTag += tagStride();
}

void IknpOtBatchOperator::receiverExtendForBits() {
    const size_t nPacked = _choiceBitsPacked->size();
    if (nPacked == 0) return;

    const size_t m = nPacked * 64;
    const size_t blocks = (m + 127) / 128;

    // 1) pack r (choices) - fast path: each 128-bit block consumes 2 packed limbs (2*64 choices)
    std::vector<U128> rPacked(blocks, {0, 0});
    {
        size_t blk = 0;
        for (size_t p = 0; p < nPacked; p += 2) {
            const uint64_t lo = static_cast<uint64_t>((*_choiceBitsPacked)[p]);
            const uint64_t hi = (p + 1 < nPacked) ? static_cast<uint64_t>((*_choiceBitsPacked)[p + 1]) : 0ULL;
            rPacked[blk++] = {lo, hi};
        }
        // Remaining blocks (only possible if m rounded up to 128) stay 0.
    }

    // 2) PRG t0/t1
    std::array<std::vector<U128>, 128> tRows0, tRows1;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        tRows0[i].resize(blocks);
        tRows1[i].resize(blocks);
        AesCtrPrg g0((uint64_t) _senderSeeds[i][0]);
        AesCtrPrg g1((uint64_t) _senderSeeds[i][1]);
        g0.generateBlocks(tRows0[i].data(), blocks);
        g1.generateBlocks(tRows1[i].data(), blocks);
    }

    // 3) u = t0 ^ t1 ^ r
    std::vector<int64_t> uRaw(SECURITY_PARAM * blocks * 2);
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        for (size_t blk = 0; blk < blocks; ++blk) {
            U128 u{
                tRows0[i][blk].lo ^ tRows1[i][blk].lo ^ rPacked[blk].lo,
                tRows0[i][blk].hi ^ tRows1[i][blk].hi ^ rPacked[blk].hi
            };
            size_t off = (i * blocks + blk) * 2;
            uRaw[off] = (int64_t) u.lo;
            uRaw[off + 1] = (int64_t) u.hi;
        }
    }

    auto rSendU = Comm::serverSendAsync(uRaw, 64, buildTag(_currentMsgTag));

    // 4) recv payload
    std::vector<int64_t> recvBuf;
    auto rRecv = Comm::serverReceiveAsync(
        recvBuf,
        nPacked * 2,
        64,
        buildTag(_currentMsgTag + 1)
    );

    Comm::wait(rSendU);
    Comm::wait(rRecv);

    // 5) transpose t0
    std::vector<U128> tCols0, uCols;
    transpose128xNBlocks(tRows0, tCols0, blocks); {
        std::array<std::vector<U128>, 128> rowsU;
        for (int i = 0; i < SECURITY_PARAM; ++i) {
            rowsU[i].resize(blocks);
            for (size_t blk = 0; blk < blocks; ++blk) {
                size_t off = (i * blocks + blk) * 2;
                rowsU[i][blk] = {
                    (uint64_t) uRaw[off],
                    (uint64_t) uRaw[off + 1]
                };
            }
        }
        transpose128xNBlocks(rowsU, uCols, blocks);
    }

    // 6) bit-sliced select
    _results.resize(nPacked);
    for (size_t p = 0; p < nPacked; ++p) {
        int64_t choice = (*_choiceBitsPacked)[p];
        int64_t mask = (choice == 0 ? 0LL : -1LL);

        int64_t block0 = recvBuf[p * 2];
        int64_t block1 = recvBuf[p * 2 + 1];

        int64_t selected = (block0 & ~mask) | (block1 & mask);

        int64_t pad = hash64(p * 64,
                             (int64_t) (tCols0[p * 64].lo ^ tCols0[p * 64].hi)
        );

        _results[p] = selected ^ pad;
    }

    _currentMsgTag += tagStride();
}

IknpOtBatchOperator *IknpOtBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start = 0;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (!_baseReady) {
        prepareBaseSeeds();
        deriveSenderSeeds();
        _baseReady = true;
    }

    // TEMP test path: expand packed bits (choices + messages) to per-bit vectors,
    // run scalar senderExtend/receiverExtend (m = nPacked*64), then (receiver) pack results back.
    std::vector<int64_t> expandedMs0;
    std::vector<int64_t> expandedMs1;
    std::vector<int> expandedChoices;

    std::vector<int64_t> *savedMs0Ptr = _ms0;
    std::vector<int64_t> *savedMs1Ptr = _ms1;
    std::vector<int> *savedChoicesPtr = _choices;
    const int savedWidth = _width;

    bool usingTempExpand = false;
    size_t nPacked = 0;

    if (_doBits) {
        usingTempExpand = true;
        _width = 1;

        if (_isSender) {
            if (!_ms0 || !_ms1 || _ms0->size() != _ms1->size()) {
                throw std::invalid_argument("IKNP(bits) temp-expand sender: ms0/ms1 null or size mismatch");
            }
            nPacked = _ms0->size();
            expandMasksPacked64(*_ms0, expandedMs0);
            expandMasksPacked64(*_ms1, expandedMs1);
            _ms0 = &expandedMs0;
            _ms1 = &expandedMs1;
            // sender doesn't use _choices
        } else {
            if (!_choiceBitsPacked) {
                throw std::invalid_argument("IKNP(bits) temp-expand receiver: choiceBitsPacked is null");
            }
            nPacked = _choiceBitsPacked->size();
            expandChoicesPacked64(*_choiceBitsPacked, expandedChoices);
            _choices = &expandedChoices;
            // receiver doesn't use _ms0/_ms1
        }
    }

    if (_isSender) {
        senderExtend();
    } else {
        receiverExtend();
    }

    if (usingTempExpand) {
        // restore pointers first
        _ms0 = savedMs0Ptr;
        _ms1 = savedMs1Ptr;
        _choices = savedChoicesPtr;
        _width = savedWidth;

        // receiver side: pack per-bit results to 64-bit limbs so callers see packed output
        if (!_isSender) {
            std::vector<int64_t> packed;
            packResultsToBits64(_results, packed, nPacked);
            _results = std::move(packed);
        }
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

void IknpOtBatchOperator::prepareBaseSeeds() {
    // Minimal: derive 128 seeds from existing ROT data.
    // We rely on IntermediateDataSupport::prepareRot() being called in System init.
    if (!IntermediateDataSupport::_sRot0 || !IntermediateDataSupport::_rRot0 ||
        !IntermediateDataSupport::_sRot1 || !IntermediateDataSupport::_rRot1) {
        throw std::runtime_error("IKNP: ROT not prepared; call IntermediateDataSupport::init() first");
    }

    _baseSeeds.resize(SECURITY_PARAM);

    // Combine the two ROT instances to form 2 seeds per i.
    // This is a pragmatic seed source for this repo (not a standard base OT transcript).
    // Each seed is 64-bit here, expanded via AES-CTR PRG.
    const uint64_t s00 = static_cast<uint64_t>(IntermediateDataSupport::_sRot0->_r0);
    const uint64_t s01 = static_cast<uint64_t>(IntermediateDataSupport::_sRot0->_r1);
    const uint64_t s10 = static_cast<uint64_t>(IntermediateDataSupport::_sRot1->_r0);
    const uint64_t s11 = static_cast<uint64_t>(IntermediateDataSupport::_sRot1->_r1);

    for (int i = 0; i < SECURITY_PARAM; ++i) {
        const uint64_t mix = (static_cast<uint64_t>(i) + 1) * 0x9e3779b97f4a7c15ULL;
        _baseSeeds[i] = {
            static_cast<int64_t>(s00 ^ s10 ^ mix),
            static_cast<int64_t>(s01 ^ s11 ^ (mix << 1))
        };
    }
}

void IknpOtBatchOperator::deriveSenderSeeds() {
    _senderSeeds.resize(SECURITY_PARAM);

    // Domain separation: derive expansion id from tags.
    const uint64_t expansionId =
            (static_cast<uint64_t>(static_cast<uint32_t>(_taskTag)) << 32) ^
            static_cast<uint64_t>(static_cast<uint32_t>(_startMsgTag));

    for (int i = 0; i < SECURITY_PARAM; ++i) {
        const auto &b = _baseSeeds[i];
        const int64_t s0 = static_cast<int64_t>(static_cast<uint64_t>(b[0]) ^ expansionId ^ (
                                                    static_cast<uint64_t>(i) << 32));
        const int64_t s1 = static_cast<int64_t>(static_cast<uint64_t>(b[1]) ^ expansionId ^ (
                                                    static_cast<uint64_t>(i) << 32));
        _senderSeeds[i] = {hash64(i, s0), hash64(i, s1)};
    }
}

void IknpOtBatchOperator::senderExtend() {
    const int m = static_cast<int>(_ms0 ? _ms0->size() : 0);
    if (m <= 0) return;

    if (!_ms1 || _ms1->size() != _ms0->size()) {
        throw std::invalid_argument("IKNP sender: ms0/ms1 size mismatch");
    }

    const size_t blocks = (static_cast<size_t>(m) + 127) / 128;

    // 1) receive u
    std::vector<int64_t> uRaw;
    auto rU = Comm::serverReceiveAsync(uRaw, static_cast<int>(SECURITY_PARAM * blocks * 2), 64,
                                       buildTag(_currentMsgTag));
    Comm::wait(rU);

    std::array<std::vector<U128>, 128> rowsU;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        rowsU[i].resize(blocks);
        for (size_t blk = 0; blk < blocks; ++blk) {
            const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
            rowsU[i][blk] = {static_cast<uint64_t>(uRaw[off]), static_cast<uint64_t>(uRaw[off + 1])};
        }
    }

    // 2) generate q0 rows
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

    // 3) Send masked payloads
    std::vector<int64_t> sendBuf(static_cast<size_t>(m) * 2);
    for (int j = 0; j < m; ++j) {
        const U128 q0 = qCols0[static_cast<size_t>(j)];
        const U128 u = uCols[static_cast<size_t>(j)];
        const U128 q1 = {q0.lo ^ u.lo, q0.hi ^ u.hi};

        const int64_t pad0 = hash64(j, static_cast<int64_t>(q0.lo ^ q0.hi));
        const int64_t pad1 = hash64(j, static_cast<int64_t>(q1.lo ^ q1.hi));

        sendBuf[static_cast<size_t>(j) * 2] = Math::ring((*_ms0)[j] ^ pad0, _width);
        sendBuf[static_cast<size_t>(j) * 2 + 1] = Math::ring((*_ms1)[j] ^ pad1, _width);
    }

    auto rSend = Comm::serverSendAsync(sendBuf, 64, buildTag(_currentMsgTag + 1));
    Comm::wait(rSend);

    _currentMsgTag += tagStride();
}

void IknpOtBatchOperator::receiverExtend() {
    const int m = _choices ? static_cast<int>(_choices->size()) : 0;
    if (m <= 0) return;

    const size_t blocks = (static_cast<size_t>(m) + 127) / 128;

    // Pack r (choice bits) into 128-bit blocks
    std::vector<U128> rPacked(blocks, U128{0, 0});
    for (int j = 0; j < m; ++j) {
        const size_t blk = static_cast<size_t>(j) / 128;
        const int bit = j % 128;
        const uint64_t b = ((*_choices)[j] == 0) ? 0ULL : 1ULL;
        if (bit < 64) rPacked[blk].lo |= (b << bit);
        else rPacked[blk].hi |= (b << (bit - 64));
    }

    // Generate t0/t1 rows using both seeds
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

    // Build u = t0 ^ t1 ^ r
    std::vector<int64_t> uRaw;
    uRaw.resize(static_cast<size_t>(SECURITY_PARAM) * blocks * 2);
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        for (size_t blk = 0; blk < blocks; ++blk) {
            const U128 u = {
                tRows0[i][blk].lo ^ tRows1[i][blk].lo ^ rPacked[blk].lo,
                tRows0[i][blk].hi ^ tRows1[i][blk].hi ^ rPacked[blk].hi
            };
            const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
            uRaw[off] = static_cast<int64_t>(u.lo);
            uRaw[off + 1] = static_cast<int64_t>(u.hi);
        }
    }

    auto rSendU = Comm::serverSendAsync(uRaw, 64, buildTag(_currentMsgTag));

    // Receive payloads
    std::vector<int64_t> recvBuf;
    auto rRecv = Comm::serverReceiveAsync(recvBuf, m * 2, 64, buildTag(_currentMsgTag + 1));

    Comm::wait(rSendU);
    Comm::wait(rRecv);

    // Transpose t0 to columns
    std::vector<U128> tCols0;
    transpose128xNBlocks(tRows0, tCols0, blocks);

    // Also need uCols to compute q when choice=1
    std::vector<U128> uCols; {
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

        const int64_t pad = hash64(j, static_cast<int64_t>(q.lo ^ q.hi));
        const int64_t masked = recvBuf[static_cast<size_t>(j) * 2 + static_cast<size_t>(c)];
        _results[j] = Math::ring(masked ^ pad, _width);
    }

    _currentMsgTag += tagStride();
}

int64_t IknpOtBatchOperator::hash64(int index, int64_t v) {
    std::hash<int64_t> h;
    const int64_t x = v ^ (static_cast<int64_t>(index) * 0x9e3779b97f4a7c15ULL);
    return h(x) ^ h(v) ^ h(index);
}
