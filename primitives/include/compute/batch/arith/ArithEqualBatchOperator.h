
#ifndef ATCHOPERATOR_H
#define ATCHOPERATOR_H

#include "ArithBatchOperator.h"

class ArithEqualBatchOperator : public ArithBatchOperator {
public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    ArithEqualBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                           int clientRank);

    ~ArithEqualBatchOperator() override = default;

    ArithEqualBatchOperator *execute() override;

    ArithEqualBatchOperator *reconstruct(int clientRank) override;

    static int tagStride(int width);
};

#endif
