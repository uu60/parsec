#ifndef IKNP_OT_BATCH_OPERATOR_H
#define IKNP_OT_BATCH_OPERATOR_H

#include "./base/AbstractOtBatchOperator.h"

#include <atomic>
#include <cstdint>
#include <vector>

class IknpOtBatchOperator : public AbstractOtBatchOperator {
public:
    inline static std::atomic_int64_t _totalTime = 0;

    static constexpr int SECURITY_PARAM = 128;

    bool _baseOtInitialized{};
    std::vector<std::vector<int64_t>> _senderSeeds{};
    int _extendedOtCount{};

    // Packed-bits OT mode (64 choice bits per int64 limb). When enabled, receiver provides
    // choice bits as packed limbs and sender provides ms0/ms1 as packed limbs.
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

    // Packed-bits constructor: choices are given as packed limbs (each limb contains 64 choice bits).
    // In this mode, width is fixed to 64.
    IknpOtBatchOperator(int sender,
                        std::vector<int64_t> *bits0Packed,
                        std::vector<int64_t> *bits1Packed,
                        std::vector<int64_t> *choiceBitsPacked,
                        int taskTag,
                        int msgTagOffset);

    IknpOtBatchOperator *execute() override;

    static int tagStride();

private:
    void initializeBaseOt();
    void senderExtend();
    void receiverExtend();

    int64_t hashFunction(int index, int64_t value);
};

#endif
