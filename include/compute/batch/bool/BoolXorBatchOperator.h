//
// Created by 杜建璋 on 25-5-26.
//

#ifndef BOOLXORBATCHOPERATOR_H
#define BOOLXORBATCHOPERATOR_H
#include "BoolBatchOperator.h"


class BoolXorBatchOperator : public BoolBatchOperator {
public:
    BoolXorBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                         int clientRank) : BoolBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    }

    BoolXorBatchOperator *execute() override;

    int msgCount();
};


#endif //BOOLXORBATCHOPERATOR_H
