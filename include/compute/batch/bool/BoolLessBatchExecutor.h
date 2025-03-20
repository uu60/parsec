//
// Created by 杜建璋 on 2025/3/17.
//

#ifndef BOOLLESSBATCHEXECUTOR_H
#define BOOLLESSBATCHEXECUTOR_H
#include "BoolBatchExecutor.h"
#include "intermediate/item/BitwiseBmt.h"


class BoolLessBatchExecutor : public BoolBatchExecutor {
private:
    // BitwiseBmt *_bmt{};
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    // reverse x and y to obey less than logic
    BoolLessBatchExecutor(std::vector<int64_t> &xs, std::vector<int64_t> &ys, int width, int taskTag, int msgTagOffset,
                          int clientRank) : BoolBatchExecutor(ys, xs, width, taskTag, msgTagOffset, clientRank) {
    }

    BoolLessBatchExecutor *execute() override;

    BoolLessBatchExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int msgTagCount(int num, int width);

    static int bmtCount(int num, int width);

private:
    std::vector<int64_t> shiftGreater(std::vector<int64_t> &in, int r) const;

    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif //BOOLLESSBATCHEXECUTOR_H
