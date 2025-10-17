
#ifndef BOOLLESSBATCHEXECUTOR_H
#define BOOLLESSBATCHEXECUTOR_H
#include "BoolBatchOperator.h"
#include "intermediate/item/BitwiseBmt.h"


class BoolLessBatchOperator : public BoolBatchOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    BoolLessBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                          int clientRank) : BoolBatchOperator(ys, xs, width, taskTag, msgTagOffset, clientRank) {
    }

    BoolLessBatchOperator *execute() override;

    BoolLessBatchOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int tagStride();

    static int bmtCount(int num, int width);

private:
    std::vector<int64_t> shiftGreater(std::vector<int64_t> &in, int r) const;

    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};


#endif
