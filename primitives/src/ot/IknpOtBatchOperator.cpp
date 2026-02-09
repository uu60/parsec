// IknpOtBatchOperator.cpp (optimized + faster col0/col64 extract)
//
// Performance optimizations applied in this version:
// 1) PRG: thread_local EVP_CIPHER_CTX reuse + in-place CTR on zeroed output
// 2) Bits-packed fast path: TRUE col0/col64 extraction (no full 128x128 transpose)
// 3) Bits-packed receiver: avoid constructing uRows; extract uCol0/uCol64 directly from uRaw
// 4) Reduce alloc churn: reuse large buffers with thread_local vectors (no header changes)

#include "ot/IknpOtBatchOperator.h"

#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <array>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <openssl/evp.h>
#include <stdexcept>
#include <vector>

#include "ot/RandOtBatchOperator.h"

namespace {
struct U128 {
    uint64_t lo;
    uint64_t hi;
};

static_assert(sizeof(U128) == 16, "U128 must be 16 bytes");
static_assert(alignof(U128) >= alignof(uint64_t), "U128 alignment unexpected");

// ---------------- Full 128x128 transpose (kept for scalar path) ----------------
static inline void transpose128x128_inplace(U128 v[128]) {
    uint64_t x0[128];
    uint64_t x1[128];
    for (int i = 0; i < 128; ++i) {
        x0[i] = v[i].lo;
        x1[i] = v[i].hi;
    }

    auto transpose64 = [](uint64_t x[128]) {
        static constexpr uint64_t m1  = 0x5555555555555555ULL;
        static constexpr uint64_t m2  = 0x3333333333333333ULL;
        static constexpr uint64_t m4  = 0x0f0f0f0f0f0f0f0fULL;
        static constexpr uint64_t m8  = 0x00ff00ff00ff00ffULL;
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

        swap_rows(1,  m1);
        swap_rows(2,  m2);
        swap_rows(4,  m4);
        swap_rows(8,  m8);
        swap_rows(16, m16);
        swap_rows(32, m32);
    };

    transpose64(x0);
    transpose64(x1);

    for (int i = 0; i < 64; ++i) {
        std::swap(x0[i + 64], x1[i]);
    }

    for (int i = 0; i < 128; ++i) {
        v[i] = {x0[i], x1[i]};
    }
}

// Full transpose path (scalar m-per-bit).
static void transpose128xNBlocks_full(const std::array<std::vector<U128>, 128> &rows,
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

// ---------------- Fast extraction: ONLY col0 and col64, NO full transpose ----------------
//
// Interpretation:
// - We have 128 rows (r=0..127), each row is a 128-bit word (lo bits [0..63], hi bits [64..127]).
// - Column 0 is: bit r = row[r].bit0  => row[r].lo & 1
// - Column 64 is: bit r = row[r].bit64 => row[r].hi & 1   (since bit64 is hi's bit0)
//
// We output U128 col where:
//   col.lo bit r (0..63)   = columnBit(r)
//   col.hi bit (r-64)      = columnBit(r) for r=64..127
//
static inline U128 extract_col0_from_rows(const U128 rows[128]) {
    uint64_t lo = 0, hi = 0;
    for (int r = 0; r < 64; ++r) {
        lo |= (rows[r].lo & 1ULL) << r;
    }
    for (int r = 64; r < 128; ++r) {
        hi |= (rows[r].lo & 1ULL) << (r - 64);
    }
    return U128{lo, hi};
}

static inline U128 extract_col64_from_rows(const U128 rows[128]) {
    uint64_t lo = 0, hi = 0;
    for (int r = 0; r < 64; ++r) {
        lo |= (rows[r].hi & 1ULL) << r;
    }
    for (int r = 64; r < 128; ++r) {
        hi |= (rows[r].hi & 1ULL) << (r - 64);
    }
    return U128{lo, hi};
}

// rows is row-major contiguous: rows[r*blocks + blk] is row r at block blk.
static inline void extract128xNBlocks_col0_col64_fast(
    const U128* rows, size_t blocks,
    U128* outCol0, U128* outCol64
) {
    U128 tile[128];
    for (size_t blk = 0; blk < blocks; ++blk) {
        const U128* base = rows + blk; // then + r*blocks
        for (int r = 0; r < 128; ++r) {
            tile[r] = base[static_cast<size_t>(r) * blocks];
        }
        outCol0[blk]  = extract_col0_from_rows(tile);
        outCol64[blk] = extract_col64_from_rows(tile);
    }
}

// Specialized fast extraction from uRaw (int64_t layout: [row][blk][lo/hi])
// uRaw has length SECURITY_PARAM * blocks * 2; row i, block blk:
//   off = (i*blocks + blk)*2;  lo=uRaw[off], hi=uRaw[off+1]
static inline void extract_uRaw_col0_col64_fast(
    const int64_t* uRaw, size_t blocks,
    U128* outCol0, U128* outCol64
) {
    U128 tile[128];

    for (size_t blk = 0; blk < blocks; ++blk) {
        for (int r = 0; r < 128; ++r) {
            const size_t off = (static_cast<size_t>(r) * blocks + blk) * 2;
            tile[r].lo = static_cast<uint64_t>(uRaw[off]);
            tile[r].hi = static_cast<uint64_t>(uRaw[off + 1]);
        }
        outCol0[blk]  = extract_col0_from_rows(tile);
        outCol64[blk] = extract_col64_from_rows(tile);
    }
}

// ---------------- PRG ----------------
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

    inline void generateBlocks(U128* out, size_t blocks) const {
        if (blocks == 0) return;

        thread_local EVP_CIPHER_CTX* ctx = nullptr;
        if (!ctx) {
            ctx = EVP_CIPHER_CTX_new();
            if (!ctx) throw std::runtime_error("EVP_CIPHER_CTX_new failed");
        }

        if (EVP_EncryptInit_ex(ctx, EVP_aes_128_ctr(), nullptr, _key.data(), _iv.data()) != 1) {
            throw std::runtime_error("EVP_EncryptInit_ex failed");
        }

        unsigned char* bytes = reinterpret_cast<unsigned char*>(out);
        const size_t nbytes = blocks * 16;

        std::memset(bytes, 0, nbytes);

        int outLen = 0;
        if (EVP_EncryptUpdate(ctx, bytes, &outLen, bytes, static_cast<int>(nbytes)) != 1) {
            throw std::runtime_error("EVP_EncryptUpdate failed");
        }
    }

private:
    std::array<unsigned char, 16> _key{};
    std::array<unsigned char, 16> _iv{};
};

} // namespace

// ---------------- Constructors ----------------
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
    agent = new RandOtBatchOperator(sender, bits0Packed, bits1Packed, choiceBitsPacked, taskTag, msgTagOffset);
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
    agent = new RandOtBatchOperator(sender, ms0, ms1, choices, width, taskTag, msgTagOffset);
}

int IknpOtBatchOperator::tagStride() {
    return 2;
}

// ---------------- Bits-packed sender (faster col extract + buffer reuse) ----------------
void IknpOtBatchOperator::senderExtendForBits() {
    const size_t nPacked = _ms0->size();
    if (nPacked == 0) return;
    if (!_ms1 || _ms1->size() != _ms0->size()) {
        throw std::invalid_argument("IKNP bits sender: ms0/ms1 size mismatch");
    }
    if (_senderSeeds.size() != SECURITY_PARAM) {
        throw std::runtime_error("IKNP: sender seeds not ready");
    }

    const size_t m = nPacked * 64;
    const size_t blocks = (m + 127) / 128;

    // Reuse big buffers to reduce alloc churn (thread_local, no header change)
    thread_local std::vector<U128> qRowsTL;
    thread_local std::vector<U128> qCol0TL;
    thread_local std::vector<U128> qCol64TL;
    thread_local std::vector<U128> uCol0TL;
    thread_local std::vector<U128> uCol64TL;
    thread_local std::vector<int64_t> sendBufTL;

    qRowsTL.resize(static_cast<size_t>(SECURITY_PARAM) * blocks);
    qCol0TL.resize(blocks);
    qCol64TL.resize(blocks);
    uCol0TL.resize(blocks);
    uCol64TL.resize(blocks);
    sendBufTL.resize(nPacked * 2);

    // 1) async receive uRaw
    std::vector<int64_t> uRaw;
    auto rU = Comm::serverReceiveAsync(
        uRaw,
        SECURITY_PARAM * blocks * 2,
        64,
        buildTag(_currentMsgTag)
    );

    // 2) generate qRows while u in flight
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        AesCtrPrg gen(static_cast<uint64_t>(_senderSeeds[i][0]));
        gen.generateBlocks(&qRowsTL[static_cast<size_t>(i) * blocks], blocks);
    }

    // 3) FAST extract q col0/col64 (no full transpose)
    extract128xNBlocks_col0_col64_fast(
        qRowsTL.data(), blocks,
        qCol0TL.data(), qCol64TL.data()
    );

    // 4) wait u, then FAST extract u col0/col64 directly from uRaw layout
    Comm::wait(rU);

    extract_uRaw_col0_col64_fast(
        uRaw.data(), blocks,
        uCol0TL.data(), uCol64TL.data()
    );

    // 5) build payload
    for (size_t p = 0; p < nPacked; ++p) {
        const size_t blk = p >> 1;
        const bool odd = (p & 1) != 0;

        const U128 q0 = odd ? qCol64TL[blk] : qCol0TL[blk];
        const U128 u  = odd ? uCol64TL[blk] : uCol0TL[blk];
        const U128 q1 = {q0.lo ^ u.lo, q0.hi ^ u.hi};

        const size_t j = p * 64;
        const int64_t pad0 = hash64(static_cast<int>(j), static_cast<int64_t>(q0.lo ^ q0.hi));
        const int64_t pad1 = hash64(static_cast<int>(j), static_cast<int64_t>(q1.lo ^ q1.hi));

        sendBufTL[p * 2]     = (*_ms0)[p] ^ pad0;
        sendBufTL[p * 2 + 1] = (*_ms1)[p] ^ pad1;
    }

    auto rSend = Comm::serverSendAsync(
        sendBufTL,
        64,
        buildTag(_currentMsgTag + 1)
    );
    Comm::wait(rSend);

    _currentMsgTag += tagStride();
}

// ---------------- Bits-packed receiver (avoid uRows build + fast extracts + buffer reuse) ----------------
void IknpOtBatchOperator::receiverExtendForBits() {
    const size_t nPacked = _choiceBitsPacked->size();
    if (nPacked == 0) return;
    if (_senderSeeds.size() != SECURITY_PARAM) {
        throw std::runtime_error("IKNP: sender seeds not ready");
    }

    const size_t m = nPacked * 64;
    const size_t blocks = (m + 127) / 128;

    // thread_local buffers
    thread_local std::vector<U128> rPackedTL;
    thread_local std::vector<U128> tRows0TL;
    thread_local std::vector<U128> tRows1TL;
    thread_local std::vector<int64_t> uRawTL;
    thread_local std::vector<int64_t> recvBufTL;
    thread_local std::vector<U128> t0Col0TL;
    thread_local std::vector<U128> t0Col64TL;
    thread_local std::vector<U128> uCol0TL;
    thread_local std::vector<U128> uCol64TL;

    rPackedTL.assign(blocks, U128{0,0});
    tRows0TL.resize(static_cast<size_t>(SECURITY_PARAM) * blocks);
    tRows1TL.resize(static_cast<size_t>(SECURITY_PARAM) * blocks);
    uRawTL.resize(static_cast<size_t>(SECURITY_PARAM) * blocks * 2);
    recvBufTL.clear(); // Comm will resize
    t0Col0TL.resize(blocks);
    t0Col64TL.resize(blocks);
    uCol0TL.resize(blocks);
    uCol64TL.resize(blocks);

    // 1) pack r into 128-bit blocks (2 packed limbs per 128-bit)
    {
        size_t blk = 0;
        for (size_t p = 0; p < nPacked; p += 2) {
            const uint64_t lo = static_cast<uint64_t>((*_choiceBitsPacked)[p]);
            const uint64_t hi = (p + 1 < nPacked) ? static_cast<uint64_t>((*_choiceBitsPacked)[p + 1]) : 0ULL;
            rPackedTL[blk++] = {lo, hi};
        }
    }

    // 2) generate t0/t1 rows
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        AesCtrPrg g0(static_cast<uint64_t>(_senderSeeds[i][0]));
        AesCtrPrg g1(static_cast<uint64_t>(_senderSeeds[i][1]));
        g0.generateBlocks(&tRows0TL[static_cast<size_t>(i) * blocks], blocks);
        g1.generateBlocks(&tRows1TL[static_cast<size_t>(i) * blocks], blocks);
    }

    // 3) u = t0 ^ t1 ^ r  (store into uRawTL)
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        for (size_t blk = 0; blk < blocks; ++blk) {
            const U128 u{
                tRows0TL[static_cast<size_t>(i) * blocks + blk].lo ^
                    tRows1TL[static_cast<size_t>(i) * blocks + blk].lo ^ rPackedTL[blk].lo,
                tRows0TL[static_cast<size_t>(i) * blocks + blk].hi ^
                    tRows1TL[static_cast<size_t>(i) * blocks + blk].hi ^ rPackedTL[blk].hi
            };
            const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
            uRawTL[off]     = static_cast<int64_t>(u.lo);
            uRawTL[off + 1] = static_cast<int64_t>(u.hi);
        }
    }

    auto rSendU = Comm::serverSendAsync(uRawTL, 64, buildTag(_currentMsgTag));

    // 4) recv payload (nPacked limbs -> 2 per limb)
    auto rRecv = Comm::serverReceiveAsync(
        recvBufTL,
        nPacked * 2,
        64,
        buildTag(_currentMsgTag + 1)
    );

    // 5) while comm in flight: FAST extract t0 col0/col64
    extract128xNBlocks_col0_col64_fast(
        tRows0TL.data(), blocks,
        t0Col0TL.data(), t0Col64TL.data()
    );

    // and FAST extract u col0/col64 directly from uRawTL
    extract_uRaw_col0_col64_fast(
        uRawTL.data(), blocks,
        uCol0TL.data(), uCol64TL.data()
    );

    Comm::wait(rSendU);
    Comm::wait(rRecv);

    // 6) recover results
    _results.resize(nPacked);

    for (size_t p = 0; p < nPacked; ++p) {
        const size_t blk = p >> 1;
        const bool odd = (p & 1) != 0;
        const size_t j = p * 64;

        const U128 t0 = odd ? t0Col64TL[blk] : t0Col0TL[blk];
        const U128 u  = odd ? uCol64TL[blk]  : uCol0TL[blk];

        const uint64_t cmask = static_cast<uint64_t>((*_choiceBitsPacked)[p]);

        const U128 q1{t0.lo ^ u.lo, t0.hi ^ u.hi};

        const uint64_t qlo = (t0.lo & ~cmask) | (q1.lo & cmask);
        const uint64_t qhi = (t0.hi & ~cmask) | (q1.hi & cmask);

        const int64_t pad = hash64(static_cast<int>(j), static_cast<int64_t>(qlo ^ qhi));

        const int64_t block0 = recvBufTL[p * 2];
        const int64_t block1 = recvBufTL[p * 2 + 1];
        const int64_t sel = (block0 & ~static_cast<int64_t>(cmask)) |
                            (block1 &  static_cast<int64_t>(cmask));

        _results[p] = sel ^ pad;
    }

    _currentMsgTag += tagStride();
}

// ---------------- Execute / Seeds / Scalar paths (unchanged except using existing PRG) ----------------
IknpOtBatchOperator *IknpOtBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    // agent->execute();
    // _results = std::move(agent->_results);
    //
    // return this;
    int64_t start = 0;

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_senderSeeds.empty()) {
        deriveSenderSeeds();
    }

    if (_doBits) {
        if (_isSender) {
            senderExtendForBits();
        } else {
            receiverExtendForBits();
        }

        if (Conf::ENABLE_CLASS_WISE_TIMING) {
            _totalTime += System::currentTimeMillis() - start;
        }
        return this;
    }

    if (_isSender) {
        senderExtend();
    } else {
        receiverExtend();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

void IknpOtBatchOperator::deriveSenderSeeds() {
    _senderSeeds.resize(SECURITY_PARAM);

    if (IntermediateDataSupport::_iknpBaseSeeds.size() != SECURITY_PARAM) {
        throw std::runtime_error("IKNP: base seeds not prepared; call IntermediateDataSupport::init() first");
    }

    const uint64_t expansionId =
        (static_cast<uint64_t>(static_cast<uint32_t>(_taskTag)) << 32) ^
        static_cast<uint64_t>(static_cast<uint32_t>(_startMsgTag));

    for (int i = 0; i < SECURITY_PARAM; ++i) {
        const auto &b = IntermediateDataSupport::_iknpBaseSeeds[i];
        const int64_t s0 = static_cast<int64_t>(static_cast<uint64_t>(b[0]) ^ expansionId ^
                                                (static_cast<uint64_t>(i) << 32));
        const int64_t s1 = static_cast<int64_t>(static_cast<uint64_t>(b[1]) ^ expansionId ^
                                                (static_cast<uint64_t>(i) << 32));
        _senderSeeds[i] = {hash64(i, s0), hash64(i, s1)};
    }
}

// ---- Scalar (m-per-bit) sender/receiver kept as-is ----
void IknpOtBatchOperator::senderExtend() {
    const int m = static_cast<int>(_ms0 ? _ms0->size() : 0);
    if (m <= 0) return;

    if (!_ms1 || _ms1->size() != _ms0->size()) {
        throw std::invalid_argument("IKNP sender: ms0/ms1 size mismatch");
    }
    if (_senderSeeds.size() != SECURITY_PARAM) {
        throw std::runtime_error("IKNP: sender seeds not ready");
    }

    const size_t blocks = (static_cast<size_t>(m) + 127) / 128;

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

    std::array<std::vector<U128>, 128> qRows;
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        qRows[i].resize(blocks);
        AesCtrPrg gen(static_cast<uint64_t>(_senderSeeds[i][0]));
        gen.generateBlocks(qRows[i].data(), blocks);
    }

    std::vector<U128> qCols0;
    transpose128xNBlocks_full(qRows, qCols0, blocks);

    std::vector<U128> uCols;
    transpose128xNBlocks_full(rowsU, uCols, blocks);

    std::vector<int64_t> sendBuf(static_cast<size_t>(m) * 2);
    for (int j = 0; j < m; ++j) {
        const U128 q0 = qCols0[static_cast<size_t>(j)];
        const U128 u  = uCols[static_cast<size_t>(j)];
        const U128 q1 = {q0.lo ^ u.lo, q0.hi ^ u.hi};

        const int64_t pad0 = hash64(j, static_cast<int64_t>(q0.lo ^ q0.hi));
        const int64_t pad1 = hash64(j, static_cast<int64_t>(q1.lo ^ q1.hi));

        sendBuf[static_cast<size_t>(j) * 2]     = Math::ring((*_ms0)[j] ^ pad0, _width);
        sendBuf[static_cast<size_t>(j) * 2 + 1] = Math::ring((*_ms1)[j] ^ pad1, _width);
    }

    auto rSend = Comm::serverSendAsync(sendBuf, 64, buildTag(_currentMsgTag + 1));
    Comm::wait(rSend);

    _currentMsgTag += tagStride();
}

void IknpOtBatchOperator::receiverExtend() {
    const int m = _choices ? static_cast<int>(_choices->size()) : 0;
    if (m <= 0) return;
    if (_senderSeeds.size() != SECURITY_PARAM) {
        throw std::runtime_error("IKNP: sender seeds not ready");
    }

    const size_t blocks = (static_cast<size_t>(m) + 127) / 128;

    std::vector<U128> rPacked(blocks, U128{0, 0});
    for (int j = 0; j < m; ++j) {
        const size_t blk = static_cast<size_t>(j) / 128;
        const int bit = j % 128;
        const uint64_t b = ((*_choices)[j] == 0) ? 0ULL : 1ULL;
        if (bit < 64) rPacked[blk].lo |= (b << bit);
        else          rPacked[blk].hi |= (b << (bit - 64));
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

    std::vector<int64_t> uRaw(static_cast<size_t>(SECURITY_PARAM) * blocks * 2);
    for (int i = 0; i < SECURITY_PARAM; ++i) {
        for (size_t blk = 0; blk < blocks; ++blk) {
            const U128 u = {
                tRows0[i][blk].lo ^ tRows1[i][blk].lo ^ rPacked[blk].lo,
                tRows0[i][blk].hi ^ tRows1[i][blk].hi ^ rPacked[blk].hi
            };
            const size_t off = (static_cast<size_t>(i) * blocks + blk) * 2;
            uRaw[off]     = static_cast<int64_t>(u.lo);
            uRaw[off + 1] = static_cast<int64_t>(u.hi);
        }
    }

    auto rSendU = Comm::serverSendAsync(uRaw, 64, buildTag(_currentMsgTag));

    std::vector<int64_t> recvBuf;
    auto rRecv = Comm::serverReceiveAsync(recvBuf, m * 2, 64, buildTag(_currentMsgTag + 1));

    Comm::wait(rSendU);
    Comm::wait(rRecv);

    std::vector<U128> tCols0;
    transpose128xNBlocks_full(tRows0, tCols0, blocks);

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
        transpose128xNBlocks_full(rowsU, uCols, blocks);
    }

    _results.resize(m);
    for (int j = 0; j < m; ++j) {
        const int c = (*_choices)[j];
        const U128 t0 = tCols0[static_cast<size_t>(j)];
        const U128 u  = uCols[static_cast<size_t>(j)];
        const U128 q  = (c == 0) ? t0 : U128{t0.lo ^ u.lo, t0.hi ^ u.hi};

        const int64_t pad    = hash64(j, static_cast<int64_t>(q.lo ^ q.hi));
        const int64_t masked = recvBuf[static_cast<size_t>(j) * 2 + static_cast<size_t>(c)];
        _results[j] = Math::ring(masked ^ pad, _width);
    }

    _currentMsgTag += tagStride();
}

// ---------------- hash64 (UNCHANGED for correctness) ----------------
int64_t IknpOtBatchOperator::hash64(int index, int64_t v) {
    std::hash<int64_t> h;
    const int64_t x = v ^ (static_cast<int64_t>(index) * 0x9e3779b97f4a7c15ULL);
    return h(x) ^ h(v) ^ h(index);
}