//
// Created by 杜建璋 on 2025/3/1.
//

#ifndef BOOLMUTEXBATCHEXECUTOR_H
#define BOOLMUTEXBATCHEXECUTOR_H
#include "./BoolBatchOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolMutexBatchOperator : public BoolBatchOperator {
public:
    std::vector<int64_t> *_conds_i{};
    inline static std::atomic_int64_t _totalTime = 0;

private:
    std::vector<BitwiseBmt> *_bmts{};

    // for bidirectional
    bool _bidir{};

public:
    BoolMutexBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, std::vector<int64_t> *conds, int width,
                           int taskTag,
                           int msgTagOffset, int clientRank);

    // For sort. Do mutex [xs, ys] [ys, xs] on [conds, conds]
    BoolMutexBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, std::vector<int64_t> *conds, int width,
                           int taskTag, int msgTagOffset);

    ~BoolMutexBatchOperator() override;

    BoolMutexBatchOperator *execute() override;

    BoolMutexBatchOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int msgTagCount();

    static int bmtCount(int num);

private:
    bool prepareBmts(std::vector<BitwiseBmt> &bmts);

    void execute0();

    void executeBidirectionally();
};


#endif //BOOLMUTEXBATCHEXECUTOR_H
