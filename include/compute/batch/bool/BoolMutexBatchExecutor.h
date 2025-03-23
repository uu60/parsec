//
// Created by 杜建璋 on 2025/3/1.
//

#ifndef BOOLMUTEXBATCHEXECUTOR_H
#define BOOLMUTEXBATCHEXECUTOR_H
#include "./BoolBatchExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolMutexBatchExecutor : public BoolBatchExecutor {
public:
    std::vector<int64_t> *_conds_i{};
    bool _dc{};
    inline static std::atomic_int64_t _totalTime = 0;

private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    BoolMutexBatchExecutor(std::vector<int64_t> *xs, std::vector<int64_t> *ys, std::vector<int64_t> *conds, int width, int taskTag,
                         int msgTagOffset, int clientRank);

    ~BoolMutexBatchExecutor() override;

    BoolMutexBatchExecutor *execute() override;

    BoolMutexBatchExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int msgTagCount(int num, int width);

    static int bmtCount(int num);

private:
    bool prepareBmts(std::vector<BitwiseBmt>& bmts);
};



#endif //BOOLMUTEXBATCHEXECUTOR_H
