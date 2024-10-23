//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_FIXEDANDSHAREEXECUTOR_H
#define MPC_PACKAGE_FIXEDANDSHAREEXECUTOR_H
#include "./AbstractAndExecutor.h"

class FixedAndExecutor : public AbstractAndExecutor {
public:
    FixedAndExecutor(bool z, bool share);
    FixedAndExecutor(bool x, bool y, bool share);

protected:
    void obtainMultiplicationTriple() override;
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_FIXEDANDSHAREEXECUTOR_H
