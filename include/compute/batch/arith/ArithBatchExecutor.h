//
// Created by 杜建璋 on 2025/1/30.
//

#ifndef ARITHBATCHEXECUTOR_H
#define ARITHBATCHEXECUTOR_H
#include "../../../base/AbstractBatchExecutor.h"


class ArithBatchExecutor : public AbstractBatchExecutor {
public:
    std::vector<int64_t> _xis{};
    std::vector<int64_t> _yis{};

    ArithBatchExecutor(std::vector<int64_t>& zs, int width, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithBatchExecutor(std::vector<int64_t>& x, std::vector<int64_t>& y, int width, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithBatchExecutor *reconstruct(int clientRank) override;

    [[deprecated("This function should not be called.")]]
    ArithBatchExecutor *execute() override;
};



#endif //ARITHBATCHEXECUTOR_H
