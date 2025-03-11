//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H

#include "./ArithExecutor.h"

class ArithAddExecutor : public ArithExecutor {
public:
    ArithAddExecutor(int64_t x, int64_t y, int width, int taskTag, int msgTagOffset,
                int clientRank) : ArithExecutor(x, y, width, taskTag, msgTagOffset, clientRank) {
    }

    ArithAddExecutor *execute() override;
};


#endif //MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
