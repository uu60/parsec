//
// Created by 杜建璋 on 2025/2/24.
//

#ifndef BOOLBATCHEXECUTOR_H
#define BOOLBATCHEXECUTOR_H
#include "../../../base/AbstractBatchOperator.h"


class BoolBatchOperator : public AbstractBatchOperator {
public:
    BoolBatchOperator(std::vector<int64_t>& zs, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolBatchOperator *reconstruct(int clientRank) override;

    BoolBatchOperator *execute() override;
};



#endif //BOOLBATCHEXECUTOR_H
