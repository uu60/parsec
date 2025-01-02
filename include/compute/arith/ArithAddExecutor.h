//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H

#include "./ArithExecutor.h"

class ArithAddExecutor : public ArithExecutor {
public:
    ArithAddExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                int clientRank) : ArithExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    };

    ArithAddExecutor *execute() override;

    [[nodiscard]] static int16_t needsMsgTags();

protected:
    [[nodiscard]] std::string className() const override;
};


#endif //MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
