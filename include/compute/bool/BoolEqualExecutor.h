//
// Created by 杜建璋 on 2024/11/15.
//

#ifndef BOOLEQUALEXECUTOR_H
#define BOOLEQUALEXECUTOR_H
#
#include "./BoolExecutor.h"

class BoolEqualExecutor : public BoolExecutor {
public:
    bool _sign{};

    BoolEqualExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                       int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {}

    BoolEqualExecutor *execute() override;

    BoolEqualExecutor *reconstruct(int clientRank) override;

    [[nodiscard]] std::string className() const override;
};

#endif //BOOLEQUALEXECUTOR_H
