//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTANDSHAREEXECUTOR_H
#define MPC_PACKAGE_RSAOTANDSHAREEXECUTOR_H
#include "./AbstractAndExecutor.h"
#include "../../bmt/RsaTripleGenerator.h"

class RsaAndExecutor : public AbstractAndExecutor {
public:
    RsaAndExecutor(bool x, bool y);
    RsaAndExecutor(bool x, bool y, bool share);
protected:
    void obtainMultiplicationTriple() override;
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_RSAOTANDSHAREEXECUTOR_H
