//
// Created by 杜建璋 on 2025/1/31.
//

#ifndef RANDOTBATCHEXECUTOR_H
#define RANDOTBATCHEXECUTOR_H
#include "./base/AbstractOtBatchExecutor.h"


class RandOtBatchExecutor : public AbstractOtBatchExecutor {
public:
    inline static std::atomic_int64_t _totalTime = 0;
    bool _doBits{};
    int64_t _totalBits{};
    std::vector<int64_t> * _choiceBits{};

public:
    RandOtBatchExecutor(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1, std::vector<int> *choices,
                        int width, int taskTag, int msgTagOffset) : AbstractOtBatchExecutor(
        sender, ms0, ms1, choices, width, taskTag, msgTagOffset) {
    }

    RandOtBatchExecutor(int sender, std::vector<int64_t> *bits0, std::vector<int64_t> *bits1,
                        std::vector<int64_t> *choiceBits, int64_t totalBits, int taskTag, int msgTagOffset);

    RandOtBatchExecutor *execute() override;

    static int msgTagCount();

private:
    void execute0();

    void executeForBits();

    void executeForBitsSingleTransfer();
};


#endif //RANDOTBATCHEXECUTOR_H
