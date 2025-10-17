
#ifndef ARITHMUTEXBATCHOPERATOR_H
#define ARITHMUTEXBATCHOPERATOR_H

#include "ArithBatchOperator.h"
#include "../../../intermediate/item/Bmt.h"

class ArithMutexBatchOperator : public ArithBatchOperator {
private:
    std::vector<int64_t> _conds_i;
    std::vector<Bmt> *_bmts = nullptr;

public:
    ArithMutexBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, std::vector<int64_t> *conds, 
                           int width, int taskTag, int msgTagOffset, int clientRank);

    ~ArithMutexBatchOperator() override = default;

    ArithMutexBatchOperator *execute() override;

    ArithMutexBatchOperator *reconstruct(int clientRank) override;

    static int tagStride(int width);

    static int bmtCount(int width, int batchSize);

    ArithMutexBatchOperator *setBmts(std::vector<Bmt> *bmts);
};

#endif
