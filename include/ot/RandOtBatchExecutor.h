//
// Created by 杜建璋 on 2025/1/31.
//

#ifndef RANDOTBATCHEXECUTOR_H
#define RANDOTBATCHEXECUTOR_H
#include "./base/AbstractOtBatchExecutor.h"


class RandOtBatchExecutor : public AbstractOtBatchExecutor {
public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    RandOtBatchExecutor(int sender, std::vector<int64_t> *ms0, std::vector<int64_t> *ms1, std::vector<int> *choices, int l, int taskTag, int msgTagOffset) : AbstractOtBatchExecutor(sender, ms0, ms1, choices, l, taskTag, msgTagOffset) {}

    RandOtBatchExecutor *execute() override;

    static int msgTagCount();
};



#endif //RANDOTBATCHEXECUTOR_H
