//
// Created by 杜建璋 on 2025/2/24.
//

#ifndef BOOLBATCHEXECUTOR_H
#define BOOLBATCHEXECUTOR_H
#include "../../../base/AbstractBatchExecutor.h"


class BoolBatchExecutor : public AbstractBatchExecutor {
public:
    BoolBatchExecutor(std::vector<int64_t>& zs, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolBatchExecutor(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolBatchExecutor *reconstruct(int clientRank) override;

    BoolBatchExecutor *execute() override;
};



#endif //BOOLBATCHEXECUTOR_H
