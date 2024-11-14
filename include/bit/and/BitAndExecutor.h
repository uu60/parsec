//
// Created by 杜建璋 on 2024/8/29.
//

#ifndef MPC_PACKAGE_ABSTRACTANDSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTANDSHAREEXECUTOR_H
#include "../../bit/BitExecutor.h"

class BitAndExecutor : public BitExecutor {
protected:
    // triple
    bool _ai{};
    bool _bi{};
    bool _ci{};

public:
    BitAndExecutor(bool x, bool y);

    BitAndExecutor(bool x, bool y, bool share);

    BitAndExecutor *execute(bool reconstruct) override;

    BitAndExecutor *obtainBmt(bool ai, bool bi, bool ci);
};


#endif //MPC_PACKAGE_ABSTRACTANDSHAREEXECUTOR_H
