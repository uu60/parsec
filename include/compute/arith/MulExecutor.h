//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#include "../ArithExecutor.h"
#include "../../bmt/BMT.h"

class MulExecutor : public ArithExecutor {
protected:
    BMT _bmt{};

public:
    MulExecutor(int64_t x, int64_t y, int l, bool local);

    MulExecutor *execute() override;

    MulExecutor *setBmt(BMT bmt);
};


#endif //MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
