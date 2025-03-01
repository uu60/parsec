//
// Created by 杜建璋 on 2025/1/30.
//

#ifndef ARITHBATCHEXECUTOR_H
#define ARITHBATCHEXECUTOR_H
#include "../../../base/AbstractBatchExecutor.h"


class ArithBatchExecutor : public AbstractBatchExecutor {
public:
    ArithBatchExecutor(std::vector<int64_t>& zs, int width, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithBatchExecutor(std::vector<int64_t>& xs, std::vector<int64_t>& ys, int width, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithBatchExecutor *reconstruct(int clientRank) override;

    ArithBatchExecutor *execute() override;
};



#endif //ARITHBATCHEXECUTOR_H
