#include "ot/IknpOtBatchOperator.h"

#include "accelerate/SimdSupport.h"
#include "comm/Comm.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Crypto.h"
#include "utils/System.h"
#include <cstring>
#include <stdexcept>
#include <algorithm>
#include <future>
#include <vector>

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

// ============== Phase Timing Helpers ==============

void IknpOtBatchOperator::printTimings() {
    const int64_t total      = _totalTime.load();
    const int64_t seedDeriv  = _timeSeedDerivation.load();
    const int64_t prg        = _timePrg.load();
    const int64_t xorU       = _timeXorU.load();
    const int64_t commSend   = _timeCommSend.load();
    const int64_t commRecv   = _timeCommRecv.load();
    const int64_t transHash  = _timeTransposeHash.load();
    const int64_t unmask     = _timeUnmask.load();

    auto pct = [&](int64_t v) -> double {
        return total > 0 ? 100.0 * v / total : 0.0;
    };

    Log::i("[IknpOT] total={}ms", total);
    Log::i("  [1] SeedDerivation : {}ms ({:.1f}%)", seedDeriv, pct(seedDeriv));
    Log::i("  [1] PRG expand     : {}ms ({:.1f}%)", prg,       pct(prg));
    Log::i("  [1] XOR-U          : {}ms ({:.1f}%)", xorU,      pct(xorU));
    Log::i("  [2] Comm send      : {}ms ({:.1f}%)", commSend,  pct(commSend));
    Log::i("  [2] Comm recv      : {}ms ({:.1f}%)", commRecv,  pct(commRecv));
    Log::i("  [3] Transpose+Hash : {}ms ({:.1f}%)", transHash, pct(transHash));
    Log::i("  [4] Unmask         : {}ms ({:.1f}%)", unmask,    pct(unmask));
}

void IknpOtBatchOperator::resetTimings() {
    _totalTime.store(0);
    _timeSeedDerivation.store(0);
    _timePrg.store(0);
    _timeXorU.store(0);
    _timeCommSend.store(0);
    _timeCommRecv.store(0);
    _timeTransposeHash.store(0);
    _timeUnmask.store(0);
}

// ============== Sender Seed Derivation ==============

void IknpOtBatchOperator::deriveSenderSeeds() {
    const int64_t ts0 = System::currentTimeMillis();

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

    _timeSeedDerivation.fetch_add(System::currentTimeMillis() - ts0, std::memory_order_relaxed);
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

    // Phase 1: Post async receive for ALL U matrices, overlap with PRG for tile 0
    std::vector<int64_t> allUBuffer(totalUSize);
    {
        const int64_t tc = System::currentTimeMillis();
        // Post the receive immediately (non-blocking)
        AbstractRequest *recvReq = Comm::serverReceiveAsync(
            allUBuffer, static_cast<int>(totalUSize), 64, buildTag(_currentMsgTag.load()));
        // While U is in-flight, pre-expand PRG for tile 0 to hide latency
        {
            alignas(32) uint64_t prgSeeds[TILE_ROWS];
            for (size_t i = 0; i < TILE_ROWS; ++i) prgSeeds[i] = _senderSeeds[i][0];
            const int64_t tp = System::currentTimeMillis();
            Crypto::batchPrgGenerate(prgSeeds, TILE_ROWS, qRows, tileBlocks[0]);
            _timePrg.fetch_add(System::currentTimeMillis() - tp, std::memory_order_relaxed);
        }
        // Wait for U to arrive
        Comm::wait(recvReq);
        _timeCommRecv.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 2: Process all tiles in parallel using multiple threads
    {
        const size_t nThreads = std::min<size_t>(numTiles,
                                    std::max<size_t>(1, std::thread::hardware_concurrency()));

        // Pre-compute per-tile offsets and uBufferOffsets
        std::vector<size_t> tileOffset(numTiles), tileUOffset(numTiles);
        {
            size_t off = 0, uOff = 0;
            for (size_t t = 0; t < numTiles; ++t) {
                tileOffset[t]  = off;
                tileUOffset[t] = uOff;
                off  += std::min<size_t>(TILE_ROWS * W, totalBits - off);
                uOff += TILE_ROWS * tileBlocks[t] * 2;
            }
        }

        // Each thread gets its own scratch buffers to avoid false sharing
        auto workerSender = [&](size_t tStart, size_t tEnd) {
            alignas(32) U128 qRowsLocal[MAX_TILE_SIZE];
            alignas(32) U128 tile[TILE_ROWS];
            alignas(32) U128 tile_xor_s[TILE_ROWS];

            for (size_t t = tStart; t < tEnd; ++t) {
                const size_t offset_ = tileOffset[t];
                const size_t chunk_  = std::min<size_t>(TILE_ROWS * W, totalBits - offset_);
                const size_t blocks_ = tileBlocks[t];
                const size_t uOff_   = tileUOffset[t];

                // PRG expand
                alignas(32) uint64_t prgSeeds[TILE_ROWS];
                for (size_t i = 0; i < TILE_ROWS; ++i)
                    prgSeeds[i] = _senderSeeds[i][0] + t;
                {
                    const int64_t tp = System::currentTimeMillis();
                    Crypto::batchPrgGenerate(prgSeeds, TILE_ROWS, qRowsLocal, blocks_);
                    _timePrg.fetch_add(System::currentTimeMillis() - tp, std::memory_order_relaxed);
                }

                // XOR with U
                const U128 *uRows_ = reinterpret_cast<const U128 *>(&allUBuffer[uOff_]);
                {
                    const int64_t tx = System::currentTimeMillis();
                    if (Conf::ENABLE_SIMD) {
                        for (size_t i = 0; i < TILE_ROWS; ++i) {
                            if (senderChoices[i])
                                SimdSupport::xorU128Arrays(&qRowsLocal[i * blocks_],
                                                           &qRowsLocal[i * blocks_],
                                                           &uRows_[i * blocks_], blocks_);
                        }
                    } else {
                        for (size_t i = 0; i < TILE_ROWS; ++i) {
                            if (senderChoices[i]) {
                                for (size_t b = 0; b < blocks_; ++b) {
                                    qRowsLocal[i * blocks_ + b].lo ^= uRows_[i * blocks_ + b].lo;
                                    qRowsLocal[i * blocks_ + b].hi ^= uRows_[i * blocks_ + b].hi;
                                }
                            }
                        }
                    }
                    _timeXorU.fetch_add(System::currentTimeMillis() - tx, std::memory_order_relaxed);
                }

                // Transpose + Hash
                {
                    const int64_t th = System::currentTimeMillis();
                    for (size_t blk = 0; blk < blocks_; ++blk) {
                        for (size_t r = 0; r < TILE_ROWS; ++r)
                            tile[r] = qRowsLocal[r * blocks_ + blk];
                        Crypto::transpose128x128_inplace(tile);

                        if (Conf::ENABLE_SIMD) {
                            SimdSupport::xorU128ArrayWithConstant(tile_xor_s, tile, sBlock, TILE_ROWS);
                        } else {
                            for (size_t k = 0; k < TILE_ROWS; ++k) {
                                tile_xor_s[k].lo = tile[k].lo ^ sBlock.lo;
                                tile_xor_s[k].hi = tile[k].hi ^ sBlock.hi;
                            }
                        }

                        uint64_t h0Bits[2], h1Bits[2];
                        const size_t validInTile = std::min<size_t>(TILE_ROWS, chunk_ - blk * TILE_ROWS);
                        Crypto::hashTileBatchPair(static_cast<int>(offset_ + blk * TILE_ROWS),
                                                  tile, tile_xor_s, validInTile, h0Bits, h1Bits);

                        // h0Bits/h1Bits: bit k = LSB of hash of column k (packed, k=0..127)
                        // pos = blk + k*blocks_  →  global bit = offset_ + blk + k*blocks_
                        // limbIdx = (offset_ + blk + k*blocks_) / 64
                        // bitPos  = (offset_ + blk + k*blocks_) % 64
                        //
                        // Fast path: when blocks_==1 AND (offset_+blk) is 64-aligned,
                        // k maps exactly to bitPos → direct word XOR (2 words cover all 128 bits).
                        // General path: still process word-by-word using 64-bit chunks from h0Bits.
                        if (blocks_ == 1 && ((offset_ + blk) % 64) == 0) {
                            // Perfect alignment: k=0..63 → limb lo, k=64..127 → limb hi
                            const size_t limb0 = (offset_ + blk) / 64;
                            // limb0 covers k=0..63, limb0+1 covers k=64..127
                            _results[limb0 * 2]     = static_cast<int64_t>(
                                static_cast<uint64_t>((*_ms0)[limb0]) ^ h0Bits[0]);
                            _results[limb0 * 2 + 1] = static_cast<int64_t>(
                                static_cast<uint64_t>((*_ms1)[limb0]) ^ h1Bits[0]);
                            if (limb0 + 1 < n && validInTile > 64) {
                                _results[(limb0+1) * 2]     = static_cast<int64_t>(
                                    static_cast<uint64_t>((*_ms0)[limb0+1]) ^ h0Bits[1]);
                                _results[(limb0+1) * 2 + 1] = static_cast<int64_t>(
                                    static_cast<uint64_t>((*_ms1)[limb0+1]) ^ h1Bits[1]);
                            }
                        } else {
                            // General path: scatter bits individually
                            for (size_t k = 0; k < validInTile; ++k) {
                                const size_t pos    = blk + k * blocks_;
                                if (offset_ + pos >= totalBits) continue;
                                const size_t limbIdx = (offset_ + pos) / 64;
                                const size_t bitPos  = (offset_ + pos) % 64;
                                const uint64_t m0_bit = (static_cast<uint64_t>((*_ms0)[limbIdx]) >> bitPos) & 1;
                                const uint64_t m1_bit = (static_cast<uint64_t>((*_ms1)[limbIdx]) >> bitPos) & 1;
                                const uint64_t h0_bit = (k < 64) ? ((h0Bits[0] >> k) & 1) : ((h0Bits[1] >> (k-64)) & 1);
                                const uint64_t h1_bit = (k < 64) ? ((h1Bits[0] >> k) & 1) : ((h1Bits[1] >> (k-64)) & 1);
                                _results[limbIdx * 2]     |= static_cast<int64_t>((m0_bit ^ h0_bit) << bitPos);
                                _results[limbIdx * 2 + 1] |= static_cast<int64_t>((m1_bit ^ h1_bit) << bitPos);
                            }
                        }
                    }
                    _timeTransposeHash.fetch_add(System::currentTimeMillis() - th, std::memory_order_relaxed);
                }
            }
        };

        // Submit tiles to thread pool
        const size_t tilesPerThread = (numTiles + nThreads - 1) / nThreads;
        std::vector<std::future<void>> futures;
        futures.reserve(nThreads);
        for (size_t i = 0; i < nThreads; ++i) {
            const size_t tStart = i * tilesPerThread;
            const size_t tEnd   = std::min(tStart + tilesPerThread, numTiles);
            if (tStart >= numTiles) break;
            futures.push_back(ThreadPoolSupport::submit([=, &workerSender]() {
                workerSender(tStart, tEnd);
            }));
        }
        for (auto &f : futures) f.get();
    }

    // Phase 3: Send masked messages to receiver
    {
        const int64_t tc = System::currentTimeMillis();
        Comm::serverSend(_results, 64, buildTag(_currentMsgTag.load()));
        _timeCommSend.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
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

    // Pre-compute per-tile offsets
    std::vector<size_t> tileOffset(numTiles), tileUOffset(numTiles);
    {
        size_t off = 0, uOff = 0;
        for (size_t t = 0; t < numTiles; ++t) {
            tileOffset[t]  = off;
            tileUOffset[t] = uOff;
            const size_t chunk = std::min<size_t>(TILE_ROWS * W, totalBits - off);
            off  += chunk;
            uOff += TILE_ROWS * tileBlocks[t] * 2;
        }
    }

    // Single buffer for all U matrices (sent to sender)
    std::vector<int64_t> allUBuffer(totalUSize);
    // Cache all T0 rows so Phase 3 skips the second batchPrgGenerate
    std::vector<U128> allTBuffer(totalUSize / 2);

    const auto &baseSeeds = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpBaseSeeds0
        : IntermediateDataSupport::_iknpBaseSeeds1;

    const uint64_t derivationSalt = static_cast<uint64_t>(_taskTag) << 32 | static_cast<uint64_t>(_startMsgTag);
    std::vector<uint64_t> derivedSeeds0(TILE_ROWS);
    std::vector<uint64_t> derivedSeeds1(TILE_ROWS);
    for (size_t i = 0; i < TILE_ROWS; ++i) {
        const uint64_t mix = Crypto::hash64(static_cast<int>(i), derivationSalt);
        derivedSeeds0[i] = static_cast<uint64_t>(baseSeeds[i][0]) ^ mix;
        derivedSeeds1[i] = static_cast<uint64_t>(baseSeeds[i][1]) ^ mix;
    }

    const size_t nThreads = std::min<size_t>(numTiles,
                                std::max<size_t>(1, std::thread::hardware_concurrency()));

    // Phase 1: Compute ALL U matrices in parallel; cache T0 rows for Phase 3
    {
        auto workerPhase1 = [&](size_t tStart, size_t tEnd) {
            alignas(32) U128 tRowsLocal[MAX_TILE_SIZE];
            alignas(32) U128 uRowsLocal[MAX_TILE_SIZE];
            alignas(32) U128 cBlocks[W];

            for (size_t t = tStart; t < tEnd; ++t) {
                const size_t offset_ = tileOffset[t];
                const size_t uOff_   = tileUOffset[t];
                const size_t chunk_  = std::min<size_t>(TILE_ROWS * W, totalBits - offset_);
                const size_t blocks_ = tileBlocks[t];

                // Pack choice bits
                std::memset(cBlocks, 0, blocks_ * sizeof(U128));
                for (size_t w = 0; w < blocks_; ++w) {
                    for (size_t row = 0; row < TILE_ROWS; ++row) {
                        const size_t idx = offset_ + w + row * blocks_;
                        if (idx >= totalBits) break;
                        const size_t limbIdx = idx / 64;
                        const size_t bitPos  = idx % 64;
                        const uint64_t choiceBit = ((*_choiceBitsPacked)[limbIdx] >> bitPos) & 1;
                        if (row < 64) cBlocks[w].lo |= (choiceBit << row);
                        else          cBlocks[w].hi |= (choiceBit << (row - 64));
                    }
                }

                // PRG expand T0 and T1
                alignas(32) uint64_t prgSeeds0[TILE_ROWS];
                alignas(32) uint64_t prgSeeds1[TILE_ROWS];
                for (size_t i = 0; i < TILE_ROWS; ++i) {
                    prgSeeds0[i] = derivedSeeds0[i] + t;
                    prgSeeds1[i] = derivedSeeds1[i] + t;
                }
                {
                    const int64_t tp = System::currentTimeMillis();
                    Crypto::batchPrgGenerate(prgSeeds0, TILE_ROWS, tRowsLocal, blocks_);
                    Crypto::batchPrgGenerate(prgSeeds1, TILE_ROWS, uRowsLocal, blocks_);
                    _timePrg.fetch_add(System::currentTimeMillis() - tp, std::memory_order_relaxed);
                }

                // Save T0 for Phase 3
                std::memcpy(&allTBuffer[uOff_ / 2], tRowsLocal, TILE_ROWS * blocks_ * sizeof(U128));

                // U = T0 XOR T1 XOR c
                {
                    const int64_t tx = System::currentTimeMillis();
                    if (Conf::ENABLE_SIMD) {
                        for (size_t i = 0; i < TILE_ROWS; ++i) {
                            SimdSupport::xorU128Arrays(&uRowsLocal[i * blocks_], &uRowsLocal[i * blocks_],
                                                       &tRowsLocal[i * blocks_], blocks_);
                            for (size_t w = 0; w < blocks_; ++w) {
                                uRowsLocal[i * blocks_ + w].lo ^= cBlocks[w].lo;
                                uRowsLocal[i * blocks_ + w].hi ^= cBlocks[w].hi;
                            }
                        }
                    } else {
                        for (size_t i = 0; i < TILE_ROWS; ++i) {
                            for (size_t w = 0; w < blocks_; ++w) {
                                uRowsLocal[i * blocks_ + w].lo ^= tRowsLocal[i * blocks_ + w].lo ^ cBlocks[w].lo;
                                uRowsLocal[i * blocks_ + w].hi ^= tRowsLocal[i * blocks_ + w].hi ^ cBlocks[w].hi;
                            }
                        }
                    }
                    _timeXorU.fetch_add(System::currentTimeMillis() - tx, std::memory_order_relaxed);
                }

                std::memcpy(&allUBuffer[uOff_], uRowsLocal, TILE_ROWS * blocks_ * sizeof(U128));
            }
        };

        const size_t tilesPerThread = (numTiles + nThreads - 1) / nThreads;
        std::vector<std::future<void>> futures;
        futures.reserve(nThreads);
        for (size_t i = 0; i < nThreads; ++i) {
            const size_t tStart = i * tilesPerThread;
            const size_t tEnd   = std::min(tStart + tilesPerThread, numTiles);
            if (tStart >= numTiles) break;
            futures.push_back(ThreadPoolSupport::submit([=, &workerPhase1]() {
                workerPhase1(tStart, tEnd);
            }));
        }
        for (auto &f : futures) f.get();
    }

    // Phase 2: Send ALL U matrices async, immediately start Phase 3 (hash) in parallel
    AbstractRequest *sendReq = nullptr;
    {
        const int64_t tc = System::currentTimeMillis();
        sendReq = Comm::serverSendAsync(allUBuffer, 64, buildTag(_currentMsgTag.load()));
        (void)tc;
    }
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 3: Transpose + Hash in parallel — runs WHILE U is being sent
    {
        auto workerPhase3 = [&](size_t tStart, size_t tEnd) {
            alignas(32) U128 tile[TILE_ROWS];
            for (size_t t = tStart; t < tEnd; ++t) {
                const size_t offset_ = tileOffset[t];
                const size_t uOff_   = tileUOffset[t];
                const size_t chunk_  = std::min<size_t>(TILE_ROWS * W, totalBits - offset_);
                const size_t blocks_ = tileBlocks[t];
                const U128 *cachedT  = &allTBuffer[uOff_ / 2];

                const int64_t th = System::currentTimeMillis();
                for (size_t blk = 0; blk < blocks_; ++blk) {
                    for (size_t r = 0; r < TILE_ROWS; ++r)
                        tile[r] = cachedT[r * blocks_ + blk];
                    Crypto::transpose128x128_inplace(tile);

                    uint64_t hBits[2];
                    const size_t validInTile = std::min<size_t>(TILE_ROWS, chunk_ - blk * TILE_ROWS);
                    Crypto::hashTileBatch(static_cast<int>(offset_ + blk * TILE_ROWS), tile, validInTile, hBits);

                    if (blocks_ == 1 && ((offset_ + blk) % 64) == 0) {
                        const size_t limb0 = (offset_ + blk) / 64;
                        _results[limb0] = static_cast<int64_t>(hBits[0]);
                        if (limb0 + 1 < n && validInTile > 64)
                            _results[limb0 + 1] = static_cast<int64_t>(hBits[1]);
                    } else {
                        for (size_t k = 0; k < validInTile; ++k) {
                            const size_t pos     = blk + k * blocks_;
                            if (offset_ + pos >= totalBits) continue;
                            const size_t limbIdx = (offset_ + pos) / 64;
                            const size_t bitPos  = (offset_ + pos) % 64;
                            const uint64_t h_bit = (k < 64) ? ((hBits[0] >> k) & 1) : ((hBits[1] >> (k-64)) & 1);
                            _results[limbIdx] |= (static_cast<int64_t>(h_bit) << bitPos);
                        }
                    }
                }
                _timeTransposeHash.fetch_add(System::currentTimeMillis() - th, std::memory_order_relaxed);
            }
        };

        const size_t tilesPerThread = (numTiles + nThreads - 1) / nThreads;
        std::vector<std::future<void>> futures;
        futures.reserve(nThreads);
        for (size_t i = 0; i < nThreads; ++i) {
            const size_t tStart = i * tilesPerThread;
            const size_t tEnd   = std::min(tStart + tilesPerThread, numTiles);
            if (tStart >= numTiles) break;
            futures.push_back(ThreadPoolSupport::submit([=, &workerPhase3]() {
                workerPhase3(tStart, tEnd);
            }));
        }
        for (auto &f : futures) f.get();
    }

    // Wait for U send to complete, record send time
    {
        const int64_t tc = System::currentTimeMillis();
        Comm::wait(sendReq);
        _timeCommSend.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }

    // Phase 4: Post async receive of masked messages, then do Unmask prep while waiting
    std::vector<int64_t> maskedMessages(n * 2);
    {
        const int64_t tc = System::currentTimeMillis();
        AbstractRequest *recvReq = Comm::serverReceiveAsync(
            maskedMessages, static_cast<int>(n * 2), 64, buildTag(_currentMsgTag.load()));
        Comm::wait(recvReq);
        _timeCommRecv.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Unmask using choice bits
    {
        const int64_t tu = System::currentTimeMillis();
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
        _timeUnmask.fetch_add(System::currentTimeMillis() - tu, std::memory_order_relaxed);
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
    {
        const int64_t tc = System::currentTimeMillis();
        Comm::serverReceive(allUBuffer, 64, buildTag(_currentMsgTag.load()));
        _timeCommRecv.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
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
        {
            const int64_t tp = System::currentTimeMillis();
            Crypto::batchPrgGenerate(prgSeeds, TILE_ROWS, qRows, 1);
            _timePrg.fetch_add(System::currentTimeMillis() - tp, std::memory_order_relaxed);
        }

        // Get U matrix from batch buffer
        const U128 *uRows = reinterpret_cast<const U128 *>(&allUBuffer[uBufferOffset]);

        // XOR with U based on sender's choice bits
        {
            const int64_t tx = System::currentTimeMillis();
            for (size_t i = 0; i < TILE_ROWS; ++i) {
                if (senderChoices[i]) {
                    qRows[i].lo ^= uRows[i].lo;
                    qRows[i].hi ^= uRows[i].hi;
                }
            }
            _timeXorU.fetch_add(System::currentTimeMillis() - tx, std::memory_order_relaxed);
        }

        // Transpose the 128x128 tile + hash
        alignas(32) U128 tile[TILE_ROWS];
        std::memcpy(tile, qRows, sizeof(tile));
        {
            const int64_t th = System::currentTimeMillis();
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
            _timeTransposeHash.fetch_add(System::currentTimeMillis() - th, std::memory_order_relaxed);
        }

        uBufferOffset += TILE_ROWS * 2;
        offset += chunk;
        ++tileIdx;
    }

    // Phase 3: Send masked messages to receiver
    {
        const int64_t tc = System::currentTimeMillis();
        Comm::serverSend(_results, 64, buildTag(_currentMsgTag.load()));
        _timeCommSend.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);
}

// ============== Scalar (per-OT) Receiver ==============

void IknpOtBatchOperator::receiverExtend() {
    const size_t n = _choices->size();
    _results.resize(n);
    std::vector<uint64_t> hashValues(n);

    constexpr size_t OTS_PER_TILE = TILE_ROWS;
    const size_t numTiles = (n + OTS_PER_TILE - 1) / OTS_PER_TILE;

    const size_t totalUSize = numTiles * TILE_ROWS * 2;
    std::vector<int64_t> allUBuffer(totalUSize);
    // *** Cache T0 rows to avoid second PRG in Phase 3 ***
    std::vector<U128> allTBuffer(numTiles * TILE_ROWS);
    size_t uBufferOffset = 0;

    alignas(32) U128 tRows[TILE_ROWS];
    alignas(32) U128 uRows[TILE_ROWS];

    const auto &baseSeeds = (_senderRank == 0)
        ? IntermediateDataSupport::_iknpBaseSeeds0
        : IntermediateDataSupport::_iknpBaseSeeds1;

    const uint64_t derivationSalt = static_cast<uint64_t>(_taskTag) << 32 | static_cast<uint64_t>(_startMsgTag);
    std::vector<uint64_t> derivedSeeds0(TILE_ROWS);
    std::vector<uint64_t> derivedSeeds1(TILE_ROWS);
    for (size_t i = 0; i < TILE_ROWS; ++i) {
        const uint64_t mix = Crypto::hash64(static_cast<int>(i), derivationSalt);
        derivedSeeds0[i] = static_cast<uint64_t>(baseSeeds[i][0]) ^ mix;
        derivedSeeds1[i] = static_cast<uint64_t>(baseSeeds[i][1]) ^ mix;
    }

    // Phase 1: Compute U matrices; cache T0
    size_t offset = 0;
    size_t tileIdx = 0;
    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        U128 cBlock{0, 0};
        for (size_t j = 0; j < chunk; ++j) {
            const uint64_t choiceBit = (*_choices)[offset + j] & 1;
            if (j < 64) cBlock.lo |= (choiceBit << j);
            else        cBlock.hi |= (choiceBit << (j - 64));
        }

        alignas(32) uint64_t prgSeeds0[TILE_ROWS];
        alignas(32) uint64_t prgSeeds1[TILE_ROWS];
        for (size_t i = 0; i < TILE_ROWS; ++i) {
            prgSeeds0[i] = derivedSeeds0[i] + tileIdx;
            prgSeeds1[i] = derivedSeeds1[i] + tileIdx;
        }
        {
            const int64_t tp = System::currentTimeMillis();
            Crypto::batchPrgGenerate(prgSeeds0, TILE_ROWS, tRows, 1);
            Crypto::batchPrgGenerate(prgSeeds1, TILE_ROWS, uRows, 1);
            _timePrg.fetch_add(System::currentTimeMillis() - tp, std::memory_order_relaxed);
        }

        // Save T0 for Phase 3
        std::memcpy(&allTBuffer[tileIdx * TILE_ROWS], tRows, TILE_ROWS * sizeof(U128));

        {
            const int64_t tx = System::currentTimeMillis();
            for (size_t i = 0; i < TILE_ROWS; ++i) {
                uRows[i].lo = tRows[i].lo ^ uRows[i].lo ^ cBlock.lo;
                uRows[i].hi = tRows[i].hi ^ uRows[i].hi ^ cBlock.hi;
            }
            _timeXorU.fetch_add(System::currentTimeMillis() - tx, std::memory_order_relaxed);
        }

        std::memcpy(&allUBuffer[uBufferOffset], uRows, TILE_ROWS * sizeof(U128));
        uBufferOffset += TILE_ROWS * 2;

        offset += chunk;
        ++tileIdx;
    }

    // Phase 2: Send U matrices
    {
        const int64_t tc = System::currentTimeMillis();
        Comm::serverSend(allUBuffer, 64, buildTag(_currentMsgTag.load()));
        _timeCommSend.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Phase 3: Transpose + Hash using cached T0 (no re-PRG)
    offset = 0;
    tileIdx = 0;
    while (offset < n) {
        const size_t chunk = std::min<size_t>(OTS_PER_TILE, n - offset);

        alignas(32) U128 tile[TILE_ROWS];
        std::memcpy(tile, &allTBuffer[tileIdx * TILE_ROWS], TILE_ROWS * sizeof(U128));
        {
            const int64_t th = System::currentTimeMillis();
            Crypto::transpose128x128_inplace(tile);

            // Use hashTileBatch (one EVP_EncryptUpdate for all 128 cols)
            uint64_t hBits[2];
            Crypto::hashTileBatch(static_cast<int>(offset), tile, chunk, hBits);
            for (size_t k = 0; k < chunk; ++k) {
                const uint64_t h_bit = (k < 64) ? ((hBits[0] >> k) & 1) : ((hBits[1] >> (k - 64)) & 1);
                hashValues[offset + k] = h_bit;
            }
            _timeTransposeHash.fetch_add(System::currentTimeMillis() - th, std::memory_order_relaxed);
        }

        offset += chunk;
        ++tileIdx;
    }

    // Phase 4: Receive masked messages
    std::vector<int64_t> maskedMessages;
    {
        const int64_t tc = System::currentTimeMillis();
        Comm::serverReceive(maskedMessages, 64, buildTag(_currentMsgTag.load()));
        _timeCommRecv.fetch_add(System::currentTimeMillis() - tc, std::memory_order_relaxed);
    }
    _currentMsgTag.fetch_add(1, std::memory_order_relaxed);

    // Unmask
    {
        const int64_t tu = System::currentTimeMillis();
        for (size_t i = 0; i < n; ++i) {
            const int choice = (*_choices)[i] & 1;
            const int64_t y_c = maskedMessages[i * 2 + choice];
            _results[i] = ring(y_c) ^ ring(static_cast<int64_t>(hashValues[i]));
        }
        _timeUnmask.fetch_add(System::currentTimeMillis() - tu, std::memory_order_relaxed);
    }
}

