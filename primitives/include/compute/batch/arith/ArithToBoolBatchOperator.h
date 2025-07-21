//
// Created by 杜建璋 on 25-7-19.
//

#ifndef ARITHTOBOOLBATCHOPERATOR_H
#define ARITHTOBOOLBATCHOPERATOR_H
#include "ArithBatchOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithToBoolBatchOperator : public ArithBatchOperator {
public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    ArithToBoolBatchOperator(std::vector<int64_t> *xs, int width, int taskTag, int msgTagOffset, int clientRank);

    ~ArithToBoolBatchOperator() override {
        delete _xis;
    }

    ArithToBoolBatchOperator *execute() override;

    ArithToBoolBatchOperator *reconstruct(int clientRank) override;

    static int tagStride(int width);
};

#endif //ARITHTOBOOLBATCHOPERATOR_H
