//
// Created by 杜建璋 on 25-5-28.
//

#ifndef ARITHADDBATCHOPERATOR_H
#define ARITHADDBATCHOPERATOR_H
#include "ArithBatchOperator.h"


class ArithAddBatchOperator : public ArithBatchOperator {
public:
    ArithAddBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset,
                          int clientRank) : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    }

    ArithAddBatchOperator *execute() override;

    static int msgTagCount();
};


#endif //ARITHADDBATCHOPERATOR_H
