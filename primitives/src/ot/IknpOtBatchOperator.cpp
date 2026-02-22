#include "ot/IknpOtBatchOperator.h"

#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Crypto.h"
#include "utils/System.h"
#include <cstring>
#include <stdexcept>
#include <algorithm>

// Stack-allocated buffers for better cache performance (aligned to SIMD boundaries)
constexpr size_t MAX_TILE_WIDTH = 64;      // Maximum tile width for stack allocation
constexpr size_t TILE_ROWS = 128;          // IKNP security parameter
constexpr size_t MAX_TILE_SIZE = TILE_ROWS * MAX_TILE_WIDTH;

// ============== Constructors ==============

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
    _senderRank = sender;
    _isSender = (Comm::rank() == sender);
    if (_isSender) {
        _ms0 = bits0Packed;
        _ms1 = bits1Packed;
    } else {
        _choiceBitsPacked = choiceBitsPacked;
    }
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
    _senderRank = sender;
    _choiceBitsPacked = nullptr;
}

int IknpOtBatchOperator::tagStride() {
    return 2;  // Uses 2 tags: U matrix transfer + hash/output
}

// ============== Execute Entry Point ==============

IknpOtBatchOperator *IknpOtBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    const int64_t t0 = System::currentTimeMillis();

    if (_isSender) {
        deriveSenderSeeds();
        if (_doBits) {
            senderExtendForBits();
        } else {
            senderExtend();
        }
    } else {
        if (_doBits) {
            receiverExtendForBits();
        } else {
            receiverExtend();
        }
    }

    const int64_t t1 = System::currentTimeMillis();
    _totalTime.fetch_add(t1 - t0, std::memory_order_relaxed);

    return this;
}

// ============== Sender Seed Derivation ==============

void IknpOtBatchOperator::deriveSenderSeeds() {
    // Derive per-operator seeds from global base seeds
    // Select the correct base seeds based on sender direction
    const auto &baseSeeds = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpBaseSeeds0
        : IntermediateDataSupport::_iknpBaseSeeds1;

    if (baseSeeds.size() < SECURITY_PARAM) {
        throw std::runtime_error("IKNP: base seeds not prepared; call IntermediateDataSupport::init() first");
    }

    _senderSeeds.resize(SECURITY_PARAM);
    const uint64_t derivationSalt = static_cast<uint64_t>(_taskTag) << 32 | static_cast<uint64_t>(_startMsgTag);


    // IKNP Sender was Base OT Receiver
    // They already received the CHOSEN seed from Base OT (stored in baseSeeds[i][0])
    // The choice bit determines which seed they received, not which to select now
    for (size_t i = 0; i < SECURITY_PARAM; ++i) {
        const uint64_t mix = Crypto::hash64(static_cast<int>(i), derivationSalt);

        // Sender already has the chosen seed stored in [0] (duplicated from base OT result)
        // No selection needed - just use it
        _senderSeeds[i][0] = static_cast<uint64_t>(baseSeeds[i][0]) ^ mix;
        _senderSeeds[i][1] = 0;  // Not used
    }
}

// ============== Packed-Bits Sender (Optimized) ==============

void IknpOtBatchOperator::senderExtendForBits() {
    const size_t n = _ms0->size();  // Number of 64-bit limbs
    const size_t totalBits = n * 64;

    // Each limb contains 64 independent 1-bit OTs
    _results.resize(n * 2);  // Store [m0_limb, m1_limb] pairs
    std::fill(_results.begin(), _results.end(), 0);  // Initialize to zero

    size_t offset = 0;
    size_t tileIdx = 0;
    constexpr size_t W = MAX_TILE_WIDTH;

    // Stack-allocated buffers
    alignas(32) U128 qRows[MAX_TILE_SIZE];

    // Precompute sender's choice bits (constant for all iterations)
    const auto &senderChoices = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpSenderChoices0
        : IntermediateDataSupport::_iknpSenderChoices1;

    // Pre-construct sender's choice bit vector as a 128-bit block (delta)
    U128 sBlock{0, 0};
    for (size_t i = 0; i < 64; ++i) {
        if (senderChoices[i]) sBlock.lo |= (1ULL << i);
    }
    for (size_t i = 64; i < TILE_ROWS; ++i) {
        if (senderChoices[i]) sBlock.hi |= (1ULL << (i - 64));
    }

    // Pipeline: post first receive ahead
    std::vector<int64_t> uBuffer;
    AbstractRequest *recvReq = Comm::serverReceiveAsync(uBuffer, TILE_ROWS * W * 2, 1, buildTag(_currentMsgTag.load()));

    while (offset < totalBits) {
        const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
        const size_t blocks = (chunk + TILE_ROWS - 1) / TILE_ROWS;

        // Expand Q rows using AES-CTR PRG
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            Crypto::AesCtrPrg prg(_senderSeeds[i][0] + tileIdx);
            prg.generateBlocks(&qRows[i * blocks], blocks);
        }

        // Wait for U matrix from receiver
        Comm::wait(recvReq);
        const U128 *uRows = reinterpret_cast<const U128 *>(uBuffer.data());

        // Post next receive to overlap with computation
        if (offset + chunk < totalBits) {
            recvReq = Comm::serverReceiveAsync(uBuffer, TILE_ROWS * W * 2, 1, buildTag(_currentMsgTag.load() + 1));
        }

        // XOR with U based on sender's choice bits (from IKNP base setup)
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            if (senderChoices[i]) {
                for (size_t b = 0; b < blocks; ++b) {
                    qRows[i * blocks + b].lo ^= uRows[i * blocks + b].lo;
                    qRows[i * blocks + b].hi ^= uRows[i * blocks + b].hi;
                }
            }
        }

        // Transpose tile-by-tile and hash columns directly
        alignas(32) U128 tile[TILE_ROWS];
        alignas(32) U128 tile_xor_s[TILE_ROWS];  // Pre-compute XOR'd tiles

        for (size_t blk = 0; blk < blocks; ++blk) {
            // Extract one tile (column of blocks)
            for (size_t r = 0; r < TILE_ROWS; ++r) {
                tile[r] = qRows[r * blocks + blk];
            }

            // Transpose 128x128 tile
            Crypto::transpose128x128_inplace(tile);

            // Pre-compute XOR'd version for h1 (batch operation)
            for (size_t k = 0; k < TILE_ROWS; ++k) {
                tile_xor_s[k].lo = tile[k].lo ^ sBlock.lo;
                tile_xor_s[k].hi = tile[k].hi ^ sBlock.hi;
            }

            // Hash each column in the tile and XOR with actual message bits
            // Use fast AES-based hash instead of SHA-256
            for (size_t k = 0; k < TILE_ROWS; ++k) {
                // OT index uses strided layout: pos = blk + k * blocks (not W!)
                const size_t pos = blk + k * blocks;
                if (offset + pos >= totalBits) continue;

                const size_t limbIdx = (offset + pos) / 64;
                const size_t bitPos = (offset + pos) % 64;

                // Get sender's actual message bits
                const uint64_t m0_bit = ((*_ms0)[limbIdx] >> bitPos) & 1;
                const uint64_t m1_bit = ((*_ms1)[limbIdx] >> bitPos) & 1;

                // Use fast AES-based hash (much faster than SHA-256)
                const uint64_t h0 = Crypto::hash64Fast(static_cast<int>(offset + pos), tile[k]);
                const uint64_t h1 = Crypto::hash64Fast(static_cast<int>(offset + pos), tile_xor_s[k]);

                // XOR message bits with hashed OT outputs
                _results[limbIdx * 2] |= ((m0_bit ^ (h0 & 1)) << bitPos);
                _results[limbIdx * 2 + 1] |= ((m1_bit ^ (h1 & 1)) << bitPos);
            }
        }

        offset += chunk;
        ++tileIdx;
        _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
    }

    // Send masked messages to receiver (y0, y1 pairs for each limb)
    Comm::serverSend(_results, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
}

// ============== Packed-Bits Receiver (Optimized) ==============

void IknpOtBatchOperator::receiverExtendForBits() {
    const size_t n = _choiceBitsPacked->size();  // Number of 64-bit limbs
    const size_t totalBits = n * 64;

    _results.resize(n);
    std::fill(_results.begin(), _results.end(), 0);  // Initialize to zero

    size_t offset = 0;
    size_t tileIdx = 0;
    constexpr size_t W = MAX_TILE_WIDTH;

    // Stack-allocated buffers
    alignas(32) U128 tRows[MAX_TILE_SIZE];
    alignas(32) U128 uRows[MAX_TILE_SIZE];
    alignas(32) U128 cBlocks[W];

    const auto &baseSeeds = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpBaseSeeds0
        : IntermediateDataSupport::_iknpBaseSeeds1;

    // Apply same derivation salt as sender for consistency
    const uint64_t derivationSalt = static_cast<uint64_t>(_taskTag) << 32 | static_cast<uint64_t>(_startMsgTag);
    std::vector<uint64_t> derivedSeeds0(TILE_ROWS);
    std::vector<uint64_t> derivedSeeds1(TILE_ROWS);
    for (size_t i = 0; i < TILE_ROWS; ++i) {
        const uint64_t mix = Crypto::hash64(static_cast<int>(i), derivationSalt);
        derivedSeeds0[i] = static_cast<uint64_t>(baseSeeds[i][0]) ^ mix;
        derivedSeeds1[i] = static_cast<uint64_t>(baseSeeds[i][1]) ^ mix;
    }

    while (offset < totalBits) {
        const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
        const size_t blocks = (chunk + TILE_ROWS - 1) / TILE_ROWS;

        // Pack choice bits into column blocks using strided layout
        // cBlocks[w] gets bits at positions {offset+w, offset+w+blocks, offset+w+2*blocks, ...}
        // This corresponds to 128 consecutive rows, each contributing 1 bit
        // After transpose, OT at position (w + blocks*k) uses cBlocks[w] bit k
        std::memset(cBlocks, 0, sizeof(cBlocks));
        for (size_t w = 0; w < blocks; ++w) {
            for (size_t row = 0; row < TILE_ROWS; ++row) {
                const size_t idx = offset + w + row * blocks;  // Strided: column w, stride = blocks (not W!)
                if (idx >= totalBits) break;

                const size_t limbIdx = idx / 64;
                const size_t bitPos = idx % 64;
                const uint64_t choiceBit = ((*_choiceBitsPacked)[limbIdx] >> bitPos) & 1;

                if (row < 64) {
                    cBlocks[w].lo |= (choiceBit << row);
                } else {
                    cBlocks[w].hi |= (choiceBit << (row - 64));
                }
            }
        }

        // Expand T and U rows using two base seeds (with derivation salt applied)
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            Crypto::AesCtrPrg prg0(derivedSeeds0[i] + tileIdx);
            Crypto::AesCtrPrg prg1(derivedSeeds1[i] + tileIdx);

            prg0.generateBlocks(&tRows[i * blocks], blocks);
            prg1.generateBlocks(&uRows[i * blocks], blocks);

            // U = T0 XOR T1 XOR c
            for (size_t w = 0; w < blocks; ++w) {
                uRows[i * blocks + w].lo ^= tRows[i * blocks + w].lo ^ cBlocks[w].lo;
                uRows[i * blocks + w].hi ^= tRows[i * blocks + w].hi ^ cBlocks[w].hi;
            }
        }

        // Send U matrix to sender
        std::vector<int64_t> uBuffer(TILE_ROWS * blocks * 2);
        std::memcpy(uBuffer.data(), uRows, TILE_ROWS * blocks * sizeof(U128));
        Comm::serverSend(uBuffer, 1, buildTag(_currentMsgTag.load()));

        // Transpose T tile-by-tile and hash columns directly
        alignas(32) U128 tile[TILE_ROWS];
        for (size_t blk = 0; blk < blocks; ++blk) {
            // Extract one tile (column of blocks)
            for (size_t r = 0; r < TILE_ROWS; ++r) {
                tile[r] = tRows[r * blocks + blk];
            }

            // Transpose 128x128 tile
            Crypto::transpose128x128_inplace(tile);

            // Hash each column in the tile - use fast AES-based hash
            for (size_t k = 0; k < TILE_ROWS; ++k) {
                // OT index uses strided layout: pos = blk + k * blocks (not W!)
                const size_t pos = blk + k * blocks;
                if (offset + pos >= totalBits) continue;

                const size_t limbIdx = (offset + pos) / 64;
                const size_t bitPos = (offset + pos) % 64;

                // Use fast AES-based hash (much faster than SHA-256)
                const uint64_t h = Crypto::hash64Fast(static_cast<int>(offset + pos), tile[k]);

                // Accumulate hash bits
                _results[limbIdx] |= ((h & 1) << bitPos);
            }
        }

        offset += chunk;
        ++tileIdx;
        _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
    }

    // Receive masked messages from sender
    std::vector<int64_t> maskedMessages;
    Comm::serverReceive(maskedMessages, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Unmask using choice bits: result[i] = y_{choice[i]} XOR H(T[i])
    // Optimized: process entire limbs using bitwise operations
    for (size_t limb = 0; limb < n; ++limb) {
        const uint64_t choice = static_cast<uint64_t>((*_choiceBitsPacked)[limb]);
        const uint64_t y0 = static_cast<uint64_t>(maskedMessages[limb * 2]);
        const uint64_t y1 = static_cast<uint64_t>(maskedMessages[limb * 2 + 1]);
        const uint64_t h = static_cast<uint64_t>(_results[limb]);

        // Select y0 or y1 based on choice bits using bitwise operations
        // result = ((y0 & ~choice) | (y1 & choice)) ^ h
        // But we need bit-by-bit selection, not word selection
        // For each bit: if choice_bit=0, use y0_bit; if choice_bit=1, use y1_bit
        // This is: (y0 & ~choice) | (y1 & choice) but applied bit-by-bit
        const uint64_t selected = (y0 & ~choice) | (y1 & choice);
        _results[limb] = static_cast<int64_t>(selected ^ h);
    }
}

// ============== Scalar (per-OT) Sender ==============

void IknpOtBatchOperator::senderExtend() {
    const size_t n = _ms0->size();
    _results.resize(n * 2);  // Store y0, y1 pairs for each OT

    size_t offset = 0;
    size_t tileIdx = 0;
    // For scalar OT: each 128x128 tile provides 128 OTs (one per column after transpose)
    constexpr size_t OTS_PER_TILE = TILE_ROWS;  // 128 OTs per tile

    alignas(32) U128 qRows[TILE_ROWS];  // One 128-bit block per row

    // Pipeline: post first receive ahead
    std::vector<int64_t> uBuffer;
    AbstractRequest *recvReq = Comm::serverReceiveAsync(uBuffer, TILE_ROWS * 2, 1, buildTag(_currentMsgTag.load()));

    // Construct sender's choice bit vector as a 128-bit block (delta)
    const auto &senderChoices = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpSenderChoices0
        : IntermediateDataSupport::_iknpSenderChoices1;
    U128 sBlock{0, 0};
    for (size_t i = 0; i < 64; ++i) {
        if (senderChoices[i]) sBlock.lo |= (1ULL << i);
    }
    for (size_t i = 64; i < TILE_ROWS; ++i) {
        if (senderChoices[i]) sBlock.hi |= (1ULL << (i - 64));
    }

    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        // Expand Q rows - one 128-bit block per row
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            Crypto::AesCtrPrg prg(_senderSeeds[i][0] + tileIdx);
            prg.generateBlocks(&qRows[i], 1);
        }

        // Wait for U matrix
        Comm::wait(recvReq);
        const U128 *uRows = reinterpret_cast<const U128 *>(uBuffer.data());

        // Post next receive
        if (offset + chunk < n) {
            recvReq = Comm::serverReceiveAsync(uBuffer, TILE_ROWS * 2, 1, buildTag(_currentMsgTag.load() + 1));
        }

        // XOR with U based on sender's choice bits
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            if (senderChoices[i]) {
                qRows[i].lo ^= uRows[i].lo;
                qRows[i].hi ^= uRows[i].hi;
            }
        }

        // Transpose the 128x128 tile
        alignas(32) U128 tile[TILE_ROWS];
        std::memcpy(tile, qRows, sizeof(tile));
        Crypto::transpose128x128_inplace(tile);

        // Each column k (0..127) provides OT output for offset+k
        for (size_t k = 0; k < chunk; ++k) {
            const size_t idx = offset + k;

            // Column k is tile[k] (after transpose)
            const U128 col = tile[k];
            const U128 col_xor_s = {col.lo ^ sBlock.lo, col.hi ^ sBlock.hi};

            const uint64_t h0 = Crypto::hash64(static_cast<int>(idx), col);
            const uint64_t h1 = Crypto::hash64(static_cast<int>(idx), col_xor_s);

            // Compute y0 = m0 XOR h0, y1 = m1 XOR h1
            _results[idx * 2] = ring((*_ms0)[idx]) ^ ring(static_cast<int64_t>(h0));
            _results[idx * 2 + 1] = ring((*_ms1)[idx]) ^ ring(static_cast<int64_t>(h1));
        }

        offset += chunk;
        ++tileIdx;
        _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
    }

    // Send masked messages to receiver (y0, y1 pairs)
    Comm::serverSend(_results, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
}

// ============== Scalar (per-OT) Receiver ==============

void IknpOtBatchOperator::receiverExtend() {
    const size_t n = _choices->size();
    _results.resize(n);

    // Temporary storage for hash values
    std::vector<uint64_t> hashValues(n);

    size_t offset = 0;
    size_t tileIdx = 0;
    // For scalar OT: each 128x128 tile provides 128 OTs (one per column after transpose)
    constexpr size_t OTS_PER_TILE = TILE_ROWS;  // 128 OTs per tile

    alignas(32) U128 tRows[TILE_ROWS];  // One 128-bit block per row
    alignas(32) U128 uRows[TILE_ROWS];

    const auto &baseSeeds = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpBaseSeeds0
        : IntermediateDataSupport::_iknpBaseSeeds1;

    // Apply same derivation salt as sender for consistency
    const uint64_t derivationSalt = static_cast<uint64_t>(_taskTag) << 32 | static_cast<uint64_t>(_startMsgTag);
    std::vector<uint64_t> derivedSeeds0(TILE_ROWS);
    std::vector<uint64_t> derivedSeeds1(TILE_ROWS);
    for (size_t i = 0; i < TILE_ROWS; ++i) {
        const uint64_t mix = Crypto::hash64(static_cast<int>(i), derivationSalt);
        derivedSeeds0[i] = static_cast<uint64_t>(baseSeeds[i][0]) ^ mix;
        derivedSeeds1[i] = static_cast<uint64_t>(baseSeeds[i][1]) ^ mix;
    }

    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        // Pack choice bits into a single 128-bit block (column vector)
        // Choice bit j goes into bit position j of this block
        // This will be XORed into EVERY row of U (same column across all rows)
        U128 cBlock{0, 0};
        for (size_t j = 0; j < chunk; ++j) {
            const uint64_t choiceBit = (*_choices)[offset + j] & 1;
            if (j < 64) {
                cBlock.lo |= (choiceBit << j);
            } else {
                cBlock.hi |= (choiceBit << (j - 64));
            }
        }

        // Expand T and U rows using derived seeds - one 128-bit block per row
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            Crypto::AesCtrPrg prg0(derivedSeeds0[i] + tileIdx);
            Crypto::AesCtrPrg prg1(derivedSeeds1[i] + tileIdx);

            prg0.generateBlocks(&tRows[i], 1);
            prg1.generateBlocks(&uRows[i], 1);

            // U[i] = T0[i] XOR T1[i] XOR c (c is the same for all rows - it's the choice column)
            uRows[i].lo = tRows[i].lo ^ uRows[i].lo ^ cBlock.lo;
            uRows[i].hi = tRows[i].hi ^ uRows[i].hi ^ cBlock.hi;
        }

        // Send U matrix
        std::vector<int64_t> uBuffer(TILE_ROWS * 2);
        std::memcpy(uBuffer.data(), uRows, TILE_ROWS * sizeof(U128));
        Comm::serverSend(uBuffer, 1, buildTag(_currentMsgTag.load()));

        // Transpose T tile
        alignas(32) U128 tile[TILE_ROWS];
        std::memcpy(tile, tRows, sizeof(tile));
        Crypto::transpose128x128_inplace(tile);

        // Each column k provides OT output for offset+k
        for (size_t k = 0; k < chunk; ++k) {
            const size_t idx = offset + k;
            const U128 col = tile[k];
            const uint64_t h = Crypto::hash64(static_cast<int>(idx), col);
            hashValues[idx] = h;
        }

        offset += chunk;
        ++tileIdx;
        _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
    }

    // Receive masked messages from sender (y0, y1 pairs)
    std::vector<int64_t> maskedMessages;
    Comm::serverReceive(maskedMessages, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Unmask using choice: result[i] = y_{choice[i]} XOR H(T[i])
    for (size_t i = 0; i < n; ++i) {
        const int choice = (*_choices)[i] & 1;
        const int64_t y_c = maskedMessages[i * 2 + choice];
        _results[i] = ring(y_c) ^ ring(static_cast<int64_t>(hashValues[i]));
    }
}

