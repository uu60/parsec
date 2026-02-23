#include "ot/IknpOtBatchOperator.h"

#include "accelerate/SimdSupport.h"
#include "comm/Comm.h"
#include "conf/Conf.h"
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
    // Both ForBits and Scalar modes use batched communication:
    // - Tag 1: All U matrices (receiver -> sender)
    // - Tag 2: Masked messages (sender -> receiver)
    return 2;
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

    constexpr size_t W = MAX_TILE_WIDTH;

    // Calculate total number of tiles and buffer size
    const size_t numTiles = (totalBits + TILE_ROWS * W - 1) / (TILE_ROWS * W);
    std::vector<size_t> tileBlocks(numTiles);
    size_t totalUSize = 0;
    {
        size_t offset = 0;
        for (size_t t = 0; t < numTiles; ++t) {
            const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
            const size_t blocks = (chunk + TILE_ROWS - 1) / TILE_ROWS;
            tileBlocks[t] = blocks;
            totalUSize += TILE_ROWS * blocks * 2;
            offset += chunk;
        }
    }

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

    // Phase 1: Receive ALL U matrices in ONE communication
    std::vector<int64_t> allUBuffer;
    Comm::serverReceive(allUBuffer, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 2: Process all tiles using the received U matrices
    size_t offset = 0;
    size_t tileIdx = 0;
    size_t uBufferOffset = 0;

    while (offset < totalBits) {
        const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
        const size_t blocks = tileBlocks[tileIdx];

        // Expand Q rows using batch PRG
        alignas(32) uint64_t prgSeeds[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds[i] = _senderSeeds[i][0] + tileIdx;
        }
        Crypto::batchPrgGenerate(prgSeeds, TILE_ROWS, qRows, blocks);

        // Get U matrix from batch buffer
        const U128 *uRows = reinterpret_cast<const U128 *>(&allUBuffer[uBufferOffset]);

        // XOR with U based on sender's choice bits
        if (Conf::ENABLE_SIMD) {
            for (size_t i = 0; i < TILE_ROWS; ++i) {
                if (senderChoices[i]) {
                    SimdSupport::xorU128Arrays(&qRows[i * blocks], &qRows[i * blocks],
                                               &uRows[i * blocks], blocks);
                }
            }
        } else {
            for (size_t i = 0; i < TILE_ROWS; ++i) {
                if (senderChoices[i]) {
                    for (size_t b = 0; b < blocks; ++b) {
                        qRows[i * blocks + b].lo ^= uRows[i * blocks + b].lo;
                        qRows[i * blocks + b].hi ^= uRows[i * blocks + b].hi;
                    }
                }
            }
        }

        // Transpose tile-by-tile and hash columns directly
        alignas(32) U128 tile[TILE_ROWS];
        alignas(32) U128 tile_xor_s[TILE_ROWS];

        for (size_t blk = 0; blk < blocks; ++blk) {
            // Extract one tile (column of blocks)
            for (size_t r = 0; r < TILE_ROWS; ++r) {
                tile[r] = qRows[r * blocks + blk];
            }

            // Transpose 128x128 tile
            Crypto::transpose128x128_inplace(tile);

            // Pre-compute XOR'd version for h1
            if (Conf::ENABLE_SIMD) {
                SimdSupport::xorU128ArrayWithConstant(tile_xor_s, tile, sBlock, TILE_ROWS);
            } else {
                for (size_t k = 0; k < TILE_ROWS; ++k) {
                    tile_xor_s[k].lo = tile[k].lo ^ sBlock.lo;
                    tile_xor_s[k].hi = tile[k].hi ^ sBlock.hi;
                }
            }

            // Batch hash entire tile for h0 and h1
            uint64_t h0Bits[2], h1Bits[2];
            const size_t validInTile = std::min<size_t>(TILE_ROWS, chunk - blk * TILE_ROWS);
            Crypto::hashTileBatch(static_cast<int>(offset + blk * TILE_ROWS), tile, validInTile, h0Bits);
            Crypto::hashTileBatch(static_cast<int>(offset + blk * TILE_ROWS), tile_xor_s, validInTile, h1Bits);

            // Process results
            for (size_t k = 0; k < TILE_ROWS && (blk + k * blocks) < chunk; ++k) {
                const size_t pos = blk + k * blocks;
                if (offset + pos >= totalBits) continue;

                const size_t limbIdx = (offset + pos) / 64;
                const size_t bitPos = (offset + pos) % 64;

                const uint64_t m0_bit = ((*_ms0)[limbIdx] >> bitPos) & 1;
                const uint64_t m1_bit = ((*_ms1)[limbIdx] >> bitPos) & 1;

                const uint64_t h0_bit = (k < 64) ? ((h0Bits[0] >> k) & 1) : ((h0Bits[1] >> (k - 64)) & 1);
                const uint64_t h1_bit = (k < 64) ? ((h1Bits[0] >> k) & 1) : ((h1Bits[1] >> (k - 64)) & 1);

                _results[limbIdx * 2] |= ((m0_bit ^ h0_bit) << bitPos);
                _results[limbIdx * 2 + 1] |= ((m1_bit ^ h1_bit) << bitPos);
            }
        }

        uBufferOffset += TILE_ROWS * blocks * 2;
        offset += chunk;
        ++tileIdx;
    }

    // Phase 3: Send masked messages to receiver
    Comm::serverSend(_results, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
}

// ============== Packed-Bits Receiver (Optimized) ==============

void IknpOtBatchOperator::receiverExtendForBits() {
    const size_t n = _choiceBitsPacked->size();  // Number of 64-bit limbs
    const size_t totalBits = n * 64;

    _results.resize(n);
    std::fill(_results.begin(), _results.end(), 0);  // Initialize to zero

    constexpr size_t W = MAX_TILE_WIDTH;

    // Calculate total number of tiles needed
    const size_t numTiles = (totalBits + TILE_ROWS * W - 1) / (TILE_ROWS * W);

    // Pre-allocate buffer for ALL U matrices (batch send optimization)
    // Each tile sends TILE_ROWS * blocks * 2 int64_t, but blocks varies per tile
    // We'll compute exact size needed
    std::vector<size_t> tileBlocks(numTiles);
    size_t totalUSize = 0;
    {
        size_t offset = 0;
        for (size_t t = 0; t < numTiles; ++t) {
            const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
            const size_t blocks = (chunk + TILE_ROWS - 1) / TILE_ROWS;
            tileBlocks[t] = blocks;
            totalUSize += TILE_ROWS * blocks * 2;
            offset += chunk;
        }
    }

    // Allocate single buffer for all U matrices
    std::vector<int64_t> allUBuffer(totalUSize);
    size_t uBufferOffset = 0;

    // Stack-allocated buffers for computation
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

    // Phase 1: Compute ALL U matrices and pack into single buffer
    size_t offset = 0;
    size_t tileIdx = 0;
    while (offset < totalBits) {
        const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
        const size_t blocks = tileBlocks[tileIdx];

        // Pack choice bits into column blocks using strided layout
        std::memset(cBlocks, 0, sizeof(cBlocks));
        for (size_t w = 0; w < blocks; ++w) {
            for (size_t row = 0; row < TILE_ROWS; ++row) {
                const size_t idx = offset + w + row * blocks;
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

        // Expand T and U rows using batch PRG
        alignas(32) uint64_t prgSeeds0[TILE_ROWS];
        alignas(32) uint64_t prgSeeds1[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds0[i] = derivedSeeds0[i] + tileIdx;
            prgSeeds1[i] = derivedSeeds1[i] + tileIdx;
        }
        Crypto::batchPrgGenerate(prgSeeds0, TILE_ROWS, tRows, blocks);
        Crypto::batchPrgGenerate(prgSeeds1, TILE_ROWS, uRows, blocks);

        // U = T0 XOR T1 XOR c
        if (Conf::ENABLE_SIMD) {
            for (size_t i = 0; i < TILE_ROWS; ++i) {
                SimdSupport::xorU128Arrays(&uRows[i * blocks], &uRows[i * blocks],
                                           &tRows[i * blocks], blocks);
                for (size_t w = 0; w < blocks; ++w) {
                    uRows[i * blocks + w].lo ^= cBlocks[w].lo;
                    uRows[i * blocks + w].hi ^= cBlocks[w].hi;
                }
            }
        } else {
            for (size_t i = 0; i < TILE_ROWS; ++i) {
                for (size_t w = 0; w < blocks; ++w) {
                    uRows[i * blocks + w].lo ^= tRows[i * blocks + w].lo ^ cBlocks[w].lo;
                    uRows[i * blocks + w].hi ^= tRows[i * blocks + w].hi ^ cBlocks[w].hi;
                }
            }
        }

        // Copy U matrix to batch buffer
        const size_t uSize = TILE_ROWS * blocks * 2;
        std::memcpy(&allUBuffer[uBufferOffset], uRows, TILE_ROWS * blocks * sizeof(U128));
        uBufferOffset += uSize;

        offset += chunk;
        ++tileIdx;
    }

    // Phase 2: Send ALL U matrices in ONE communication
    Comm::serverSend(allUBuffer, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 3: Compute local hashes (no communication needed)
    offset = 0;
    tileIdx = 0;
    uBufferOffset = 0;
    while (offset < totalBits) {
        const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - offset);
        const size_t blocks = tileBlocks[tileIdx];

        // Regenerate T rows (same seeds as before)
        alignas(32) uint64_t prgSeeds0[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds0[i] = derivedSeeds0[i] + tileIdx;
        }
        Crypto::batchPrgGenerate(prgSeeds0, TILE_ROWS, tRows, blocks);

        // Transpose T tile-by-tile and hash columns directly
        alignas(32) U128 tile[TILE_ROWS];
        for (size_t blk = 0; blk < blocks; ++blk) {
            // Extract one tile (column of blocks)
            for (size_t r = 0; r < TILE_ROWS; ++r) {
                tile[r] = tRows[r * blocks + blk];
            }

            // Transpose 128x128 tile
            Crypto::transpose128x128_inplace(tile);

            // Batch hash entire tile
            uint64_t hBits[2];
            const size_t validInTile = std::min<size_t>(TILE_ROWS, chunk - blk * TILE_ROWS);
            Crypto::hashTileBatch(static_cast<int>(offset + blk * TILE_ROWS), tile, validInTile, hBits);

            // Process results
            for (size_t k = 0; k < TILE_ROWS && (blk + k * blocks) < chunk; ++k) {
                const size_t pos = blk + k * blocks;
                if (offset + pos >= totalBits) continue;

                const size_t limbIdx = (offset + pos) / 64;
                const size_t bitPos = (offset + pos) % 64;

                const uint64_t h_bit = (k < 64) ? ((hBits[0] >> k) & 1) : ((hBits[1] >> (k - 64)) & 1);
                _results[limbIdx] |= (static_cast<int64_t>(h_bit) << bitPos);
            }
        }

        uBufferOffset += TILE_ROWS * blocks * 2;
        offset += chunk;
        ++tileIdx;
    }

    // Phase 4: Receive masked messages from sender
    std::vector<int64_t> maskedMessages;
    Comm::serverReceive(maskedMessages, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Unmask using choice bits
    if (Conf::ENABLE_SIMD) {
        std::vector<int64_t> y0Vec(n), y1Vec(n);
        for (size_t limb = 0; limb < n; ++limb) {
            y0Vec[limb] = maskedMessages[limb * 2];
            y1Vec[limb] = maskedMessages[limb * 2 + 1];
        }
        SimdSupport::selectBits(_results.data(), y0Vec.data(), y1Vec.data(),
                               _choiceBitsPacked->data(), _results.data(), n);
    } else {
        for (size_t limb = 0; limb < n; ++limb) {
            const uint64_t choice = static_cast<uint64_t>((*_choiceBitsPacked)[limb]);
            const uint64_t y0 = static_cast<uint64_t>(maskedMessages[limb * 2]);
            const uint64_t y1 = static_cast<uint64_t>(maskedMessages[limb * 2 + 1]);
            const uint64_t h = static_cast<uint64_t>(_results[limb]);
            const uint64_t selected = (y0 & ~choice) | (y1 & choice);
            _results[limb] = static_cast<int64_t>(selected ^ h);
        }
    }
}

// ============== Scalar (per-OT) Sender ==============

void IknpOtBatchOperator::senderExtend() {
    const size_t n = _ms0->size();
    _results.resize(n * 2);  // Store y0, y1 pairs for each OT

    // For scalar OT: each 128x128 tile provides 128 OTs (one per column after transpose)
    constexpr size_t OTS_PER_TILE = TILE_ROWS;  // 128 OTs per tile

    alignas(32) U128 qRows[TILE_ROWS];

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

    // Phase 1: Receive ALL U matrices in ONE communication
    std::vector<int64_t> allUBuffer;
    Comm::serverReceive(allUBuffer, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 2: Process all tiles using the received U matrices
    size_t offset = 0;
    size_t tileIdx = 0;
    size_t uBufferOffset = 0;

    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        // Expand Q rows using batch PRG
        alignas(32) uint64_t prgSeeds[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds[i] = _senderSeeds[i][0] + tileIdx;
        }
        Crypto::batchPrgGenerate(prgSeeds, TILE_ROWS, qRows, 1);

        // Get U matrix from batch buffer
        const U128 *uRows = reinterpret_cast<const U128 *>(&allUBuffer[uBufferOffset]);

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

            // Use fast AES hash
            const uint64_t h0 = Crypto::hash64Fast(static_cast<int>(idx), col);
            const uint64_t h1 = Crypto::hash64Fast(static_cast<int>(idx), col_xor_s);

            // Compute y0 = m0 XOR h0, y1 = m1 XOR h1
            _results[idx * 2] = ring((*_ms0)[idx]) ^ ring(static_cast<int64_t>(h0));
            _results[idx * 2 + 1] = ring((*_ms1)[idx]) ^ ring(static_cast<int64_t>(h1));
        }

        uBufferOffset += TILE_ROWS * 2;
        offset += chunk;
        ++tileIdx;
    }

    // Phase 3: Send masked messages to receiver
    Comm::serverSend(_results, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
}

// ============== Scalar (per-OT) Receiver ==============

void IknpOtBatchOperator::receiverExtend() {
    const size_t n = _choices->size();
    _results.resize(n);

    // Temporary storage for hash values
    std::vector<uint64_t> hashValues(n);

    // For scalar OT: each 128x128 tile provides 128 OTs (one per column after transpose)
    constexpr size_t OTS_PER_TILE = TILE_ROWS;  // 128 OTs per tile

    // Calculate total number of tiles needed
    const size_t numTiles = (n + OTS_PER_TILE - 1) / OTS_PER_TILE;

    // Pre-allocate buffer for ALL U matrices (batch send optimization)
    // Each tile sends TILE_ROWS * 2 int64_t (one U128 per row)
    const size_t totalUSize = numTiles * TILE_ROWS * 2;
    std::vector<int64_t> allUBuffer(totalUSize);
    size_t uBufferOffset = 0;

    alignas(32) U128 tRows[TILE_ROWS];
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

    // Phase 1: Compute ALL U matrices and pack into single buffer
    size_t offset = 0;
    size_t tileIdx = 0;
    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        // Pack choice bits into a single 128-bit block (column vector)
        U128 cBlock{0, 0};
        for (size_t j = 0; j < chunk; ++j) {
            const uint64_t choiceBit = (*_choices)[offset + j] & 1;
            if (j < 64) {
                cBlock.lo |= (choiceBit << j);
            } else {
                cBlock.hi |= (choiceBit << (j - 64));
            }
        }

        // Expand T and U rows using batch PRG
        alignas(32) uint64_t prgSeeds0[TILE_ROWS];
        alignas(32) uint64_t prgSeeds1[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds0[i] = derivedSeeds0[i] + tileIdx;
            prgSeeds1[i] = derivedSeeds1[i] + tileIdx;
        }
        Crypto::batchPrgGenerate(prgSeeds0, TILE_ROWS, tRows, 1);
        Crypto::batchPrgGenerate(prgSeeds1, TILE_ROWS, uRows, 1);

        // U[i] = T0[i] XOR T1[i] XOR c
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            uRows[i].lo = tRows[i].lo ^ uRows[i].lo ^ cBlock.lo;
            uRows[i].hi = tRows[i].hi ^ uRows[i].hi ^ cBlock.hi;
        }

        // Copy U matrix to batch buffer
        std::memcpy(&allUBuffer[uBufferOffset], uRows, TILE_ROWS * sizeof(U128));
        uBufferOffset += TILE_ROWS * 2;

        offset += chunk;
        ++tileIdx;
    }

    // Phase 2: Send ALL U matrices in ONE communication
    Comm::serverSend(allUBuffer, 1, buildTag(_currentMsgTag.load()));
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 3: Compute local hashes (no communication needed)
    offset = 0;
    tileIdx = 0;
    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        // Regenerate T rows (same seeds as before)
        alignas(32) uint64_t prgSeeds0[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds0[i] = derivedSeeds0[i] + tileIdx;
        }
        Crypto::batchPrgGenerate(prgSeeds0, TILE_ROWS, tRows, 1);

        // Transpose T tile
        alignas(32) U128 tile[TILE_ROWS];
        std::memcpy(tile, tRows, sizeof(tile));
        Crypto::transpose128x128_inplace(tile);

        // Each column k provides OT output for offset+k
        for (size_t k = 0; k < chunk; ++k) {
            const size_t idx = offset + k;
            const U128 col = tile[k];
            const uint64_t h = Crypto::hash64Fast(static_cast<int>(idx), col);
            hashValues[idx] = h;
        }

        offset += chunk;
        ++tileIdx;
    }

    // Phase 4: Receive masked messages from sender
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

