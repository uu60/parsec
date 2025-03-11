//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#include "./ArithExecutor.h"
#include "../../../intermediate/item/Bmt.h"

class ArithMultiplyExecutor : public ArithExecutor {
private:
    Bmt* _bmt{};

public:
    ArithMultiplyExecutor(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank) : ArithExecutor(
        x, y, l, taskTag, msgTagOffset, clientRank) {
    }

    ArithMultiplyExecutor *execute() override;

    [[nodiscard]] static int msgTagCount(int width);

    ArithMultiplyExecutor *setBmt(Bmt *bmt);
};


#endif //MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
