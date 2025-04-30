//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H

#include "./ArithOperator.h"

class ArithAddOperator : public ArithOperator {
public:
    ArithAddOperator(int64_t x, int64_t y, int width, int taskTag, int msgTagOffset,
                int clientRank) : ArithOperator(x, y, width, taskTag, msgTagOffset, clientRank) {
    }

    ArithAddOperator *execute() override;
};


#endif //MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
