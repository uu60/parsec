//
// Created by 杜建璋 on 2025/3/17.
//

#ifndef BOOLLESSBATCHEXECUTOR_H
#define BOOLLESSBATCHEXECUTOR_H
#include "BoolBatchOperator.h"
#include "intermediate/item/BitwiseBmt.h"


class BoolLessBatchOperator : public BoolBatchOperator {
private:
    // BitwiseBmt *_bmt{};
    std::vector<BitwiseBmt> *_bmts{};
    bool _doBidirectional{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    // reverse x and y to obey less than logic
    BoolLessBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                          int clientRank) : BoolBatchOperator(ys, xs, width, taskTag, msgTagOffset, clientRank) {
    }

    BoolLessBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset);

    BoolLessBatchOperator *execute() override;

    BoolLessBatchOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int msgTagCount();

    static int bmtCount(int num, int width);

private:

    void execute0();

    void executeBidirectional();

    std::vector<int64_t> shiftGreater(std::vector<int64_t> &in, int r) const;

    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif //BOOLLESSBATCHEXECUTOR_H
