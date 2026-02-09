#ifndef IKNP_OT_BATCH_OPERATOR_H
#define IKNP_OT_BATCH_OPERATOR_H

#include "./base/AbstractOtBatchOperator.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <vector>

#include "RandOtBatchOperator.h"

// Minimal IKNP OT extension operator (k=128).
//
// Notes for this repo/version:
// - The codebase already has ROT prepared in IntermediateDataSupport::prepareRot().
// - To keep changes minimal and self-contained, this IKNP implementation uses 128 "base seeds"
//   derived from ROT (two directional caches are optional; this version targets sender=0/1 with 2-party servers).
// - Domain separation is derived deterministically from (taskTag, msgTagOffset) to avoid extra sync.
class IknpOtBatchOperator : public AbstractOtBatchOperator {
public:
    inline static std::atomic_int64_t _totalTime = 0;

    static constexpr int SECURITY_PARAM = 128;

    // Sender side keeps two seeds per i, derived per-operator from base seeds.
    std::vector<std::array<int64_t, 2> > _senderSeeds; // [k] derived per-batch seeds

    // Packed-bits mode (compatible with RandOtBatchOperator bits constructor)
    bool _doBits{};
    std::vector<int64_t> *_choiceBitsPacked{};

public:
    IknpOtBatchOperator(int sender,
                        std::vector<int64_t> *ms0,
                        std::vector<int64_t> *ms1,
                        std::vector<int> *choices,
                        int width,
                        int taskTag,
                        int msgTagOffset);

    // Packed-bits constructor: ms0/ms1 and choices are packed 64-bit limbs.
    // Each limb represents 64 independent 1-bit OTs; output is packed in the same format.
    IknpOtBatchOperator(int sender,
                        std::vector<int64_t> *bits0Packed,
                        std::vector<int64_t> *bits1Packed,
                        std::vector<int64_t> *choiceBitsPacked,
                        int taskTag,
                        int msgTagOffset);

    IknpOtBatchOperator *execute() override;

    // Uses 2 tags: u + payload.
    static int tagStride();

private:
    void senderExtendForBits();

    void receiverExtendForBits();

    void deriveSenderSeeds();

    void senderExtend();

    void receiverExtend();

    static int64_t hash64(int index, int64_t v);
};

#endif
