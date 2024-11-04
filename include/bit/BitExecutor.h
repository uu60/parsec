//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_BOOLSHAREEXECUTOR_H
#define MPC_PACKAGE_BOOLSHAREEXECUTOR_H

#include "../SecureExecutor.h"

class BitExecutor : public SecureExecutor<bool> {
public:
    bool _xi{};
    bool _yi{};

public:
    BitExecutor(bool z, bool share);

    BitExecutor(bool x, bool y, bool share);

    BitExecutor *reconstruct() override;

    BitExecutor *execute(bool reconstruct) override;

protected:
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_BOOLSHAREEXECUTOR_H
