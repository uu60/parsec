//
// Created by 杜建璋 on 25-5-25.
//

#ifndef BOOLEQUALBATCHOPERATOR_H
#define BOOLEQUALBATCHOPERATOR_H

#include "BoolBatchOperator.h"
#include "intermediate/item/BitwiseBmt.h"

class BoolEqualBatchOperator : public BoolBatchOperator {
private:
    // BitwiseBmt *_bmt{};
    std::vector<BitwiseBmt> *_bmts{};
    bool _doBidirectional{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    // reverse x and y to obey less than logic
    BoolEqualBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                          int clientRank) : BoolBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    }

    BoolEqualBatchOperator *execute() override;

    BoolEqualBatchOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int msgTagCount();

    static int bmtCount(int num, int width);

private:
    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};

#endif //BOOLEQUALBATCHOPERATOR_H
