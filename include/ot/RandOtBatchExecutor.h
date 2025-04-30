//
// Created by 杜建璋 on 2025/1/31.
//

#ifndef RANDOTBATCHEXECUTOR_H
#define RANDOTBATCHEXECUTOR_H
#include "./base/AbstractOtBatchOperator.h"


class RandOtBatchOperator : public AbstractOtBatchOperator {
public:
    inline static std::atomic_int64_t _totalTime = 0;
    bool _doBits{};
    std::vector<int64_t> * _choiceBits{};

public:
    RandOtBatchOperator(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1, std::vector<int> *choices,
                        int width, int taskTag, int msgTagOffset) : AbstractOtBatchOperator(
        sender, ms0, ms1, choices, width, taskTag, msgTagOffset) {
    }

    RandOtBatchOperator(int sender, std::vector<int64_t> *bits0, std::vector<int64_t> *bits1,
                        std::vector<int64_t> *choiceBits, int taskTag, int msgTagOffset);

    RandOtBatchOperator *execute() override;

    static int msgTagCount();

private:
    void execute0();

    void executeForBits();

    void executeForBitsSingleTransfer();

    /**
     * Based on executeForBitsSingleTransfer() but no synchronization after
     */
    void executeForBitsAsync();
};


#endif //RANDOTBATCHEXECUTOR_H
