//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H
#include "./ArithExecutor.h"
#include <vector>

class ArithLessThanExecutor : public ArithExecutor {
public:
    bool _sign{};

    ArithLessThanExecutor(int64_t x, int64_t y, int l, int16_t objTag, int16_t msgTagOffset, int clientRank);

    ArithLessThanExecutor *execute() override;

    ArithLessThanExecutor *reconstruct(int clientRank) override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t neededMsgTags();
};

#endif //MPC_PACKAGE_COMPAREEXECUTOR_H
