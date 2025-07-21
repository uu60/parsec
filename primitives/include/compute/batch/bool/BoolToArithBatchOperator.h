//
// Created by 杜建璋 on 25-5-27.
//

#ifndef BOOLTOARITHBATCHOPERATOR_H
#define BOOLTOARITHBATCHOPERATOR_H
#include "BoolBatchOperator.h"


class BoolToArithBatchOperator : public BoolBatchOperator {
public:
    BoolToArithBatchOperator(std::vector<int64_t> *xs, int width, int taskTag,
                             int msgTagOffset, int clientRank) : BoolBatchOperator(
        xs, nullptr, width, taskTag, msgTagOffset, clientRank) {

    };

    BoolToArithBatchOperator *execute() override;

    static int tagStride();
};


#endif //BOOLTOARITHBATCHOPERATOR_H
