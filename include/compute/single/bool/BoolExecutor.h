//
// Created by 杜建璋 on 2024/11/7.
//

#ifndef INTBOOLEXECUTOR_H
#define INTBOOLEXECUTOR_H
#include "../../../base/AbstractSingleExecutor.h"

class BoolExecutor : public AbstractSingleExecutor {
public:
    int64_t _xi{};
    int64_t _yi{};

    BoolExecutor(int64_t z, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    BoolExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    BoolExecutor *reconstruct(int clientRank) override;

    [[deprecated("This function should not be called.")]]
    BoolExecutor *execute() override;
};

#endif //INTBOOLEXECUTOR_H
