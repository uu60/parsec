
#ifndef BOOLEQUALBATCHOPERATOR_H
#define BOOLEQUALBATCHOPERATOR_H

#include "BoolBatchOperator.h"
#include "intermediate/item/BitwiseBmt.h"

class BoolEqualBatchOperator : public BoolBatchOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};
    bool _dbIn{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolEqualBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *afterCol, int width, int taskTag, int msgTagOffset,
                          int clientRank) : BoolBatchOperator(xs, afterCol, width, taskTag, msgTagOffset, clientRank) {
    }

    BoolEqualBatchOperator *execute() override;

    BoolEqualBatchOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int tagStride();

    static int bmtCount(int num, int width);

private:
    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};

#endif
