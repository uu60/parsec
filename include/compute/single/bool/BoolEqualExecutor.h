//
// Created by 杜建璋 on 2024/11/15.
//

#ifndef BOOLEQUALEXECUTOR_H
#define BOOLEQUALEXECUTOR_H
#
#include "./BoolOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolEqualOperator : public BoolOperator {
private:
    // std::vector<Bmt> *_bmts{};
    BitwiseBmt *_bmt{};

public:
    BoolEqualOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset,
                       int clientRank) : BoolOperator(x, y, l, taskTag, msgTagOffset, clientRank) {}

    BoolEqualOperator *execute() override;

    BoolEqualOperator *setBmt(BitwiseBmt *bmt);
};

#endif //BOOLEQUALEXECUTOR_H
