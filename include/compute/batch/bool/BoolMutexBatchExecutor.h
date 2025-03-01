//
// Created by 杜建璋 on 2025/3/1.
//

#ifndef BOOLMUTEXBATCHEXECUTOR_H
#define BOOLMUTEXBATCHEXECUTOR_H
#include "BoolBatchExecutor.h"
#include "intermediate/item/BitwiseBmt.h"


class BoolMutexBatchExecutor : public BoolBatchExecutor {
private:
    std::vector<int64_t> _cond_i{};
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolMutexBatchExecutor(std::vector<int64_t> &xs, std::vector<int64_t> &ys, int width, int16_t taskTag,
                         int16_t msgTagOffset, int clientRank);

    BoolMutexBatchExecutor *execute() override;

    BoolMutexBatchExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int16_t msgTagCount(int l);

    static int bmtCount(int width);
};



#endif //BOOLMUTEXBATCHEXECUTOR_H
