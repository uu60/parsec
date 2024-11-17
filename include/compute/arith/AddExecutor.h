//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H

#include "../ArithExecutor.h"

class AddExecutor : public ArithExecutor {
public:
    AddExecutor(int64_t x, int64_t y, int l, bool local);

    AddExecutor *execute() override;

protected:
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
