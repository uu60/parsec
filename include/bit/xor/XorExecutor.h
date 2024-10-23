//
// Created by 杜建璋 on 2024/8/29.
//

#ifndef MPC_PACKAGE_XORSHAREEXECUTOR_H
#define MPC_PACKAGE_XORSHAREEXECUTOR_H

#include "../../bit/BitExecutor.h"

class XorExecutor : public BitExecutor {
public:
    XorExecutor(bool z, bool share);

    XorExecutor(bool x, bool y, bool share);

    XorExecutor *execute(bool reconstruct) override;

protected:
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_XORSHAREEXECUTOR_H
