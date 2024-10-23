//
// Created by 杜建璋 on 2024/8/29.
//

#ifndef MPC_PACKAGE_ABSTRACTANDSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTANDSHAREEXECUTOR_H
#include "../../bit/BitExecutor.h"

class AbstractAndExecutor : public BitExecutor {
protected:
    // triple
    bool _ai{};
    bool _bi{};
    bool _ci{};

public:
    AbstractAndExecutor(bool x, bool y);
    AbstractAndExecutor(bool x, bool y, bool share);
    AbstractAndExecutor* execute(bool reconstruct) override;
protected:
    virtual void obtainMultiplicationTriple() = 0;
private:
    void process(bool reconstruct);
};


#endif //MPC_PACKAGE_ABSTRACTANDSHAREEXECUTOR_H
