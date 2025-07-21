//
// Created by 杜建璋 on 25-7-21.
//

#ifndef ARITHLESSBATCHOPERATOR_H
#define ARITHLESSBATCHOPERATOR_H

#include "ArithBatchOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithLessBatchOperator : public ArithBatchOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    ArithLessBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset, int clientRank);

    ~ArithLessBatchOperator() override = default;

    ArithLessBatchOperator *execute() override;

    ArithLessBatchOperator *reconstruct(int clientRank) override;

    static int tagStride(int width);
};

#endif //ARITHLESSBATCHOPERATOR_H
