#ifndef IKNP_OT_BATCH_OPERATOR_H
#define IKNP_OT_BATCH_OPERATOR_H

#include "parsec/ot/base/AbstractOtBatchOperator.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <vector>

#include "parsec/ot/RandOtBatchOperator.h"

namespace parsec {

// IKNP OT extension (k = 128) between the two servers.
//
// - 128 base OTs per direction transfer 128-bit seeds once, in IntermediateDataSupport::prepareIknp().
// - Seeds are expanded with Crypto::SeededPrg (AES-128 keyed by each seed).
// - Every execution uses a fresh 128-bit nonce chosen by the IKNP receiver and sent with its first message, so PRG
//   output is never reused across executions, independent of task and message tags.
class IknpOtBatchOperator : public AbstractOtBatchOperator {
  public:
    inline static std::atomic_int64_t _totalTime = 0;

    static constexpr int SECURITY_PARAM = 128;

    // Packed-bits mode (compatible with RandOtBatchOperator bits constructor)
    bool _doBits{};
    std::vector<int64_t>* _choiceBitsPacked{};

    // Track which rank is the IKNP sender (0 or 1) to select correct base seeds
    int _senderRank{};

  public:
    IknpOtBatchOperator(int sender, std::vector<int64_t>* ms0, std::vector<int64_t>* ms1, std::vector<int>* choices,
                        int width, int taskTag, int msgTagOffset);

    // Packed-bits constructor: ms0/ms1 and choices are packed 64-bit limbs.
    // Each limb represents 64 independent 1-bit OTs; output is packed in the same format.
    IknpOtBatchOperator(int sender, std::vector<int64_t>* bits0Packed, std::vector<int64_t>* bits1Packed,
                        std::vector<int64_t>* choiceBitsPacked, int taskTag, int msgTagOffset);

    IknpOtBatchOperator* execute() override;

    // Uses 2 tags: u + payload.
    static int tagStride();

  private:
    void senderExtendForBits();
    void receiverExtendForBits();

    void senderExtend();
    void receiverExtend();
};

} // namespace parsec

#endif
