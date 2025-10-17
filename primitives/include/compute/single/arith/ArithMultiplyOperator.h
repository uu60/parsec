
#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#include "./ArithOperator.h"
#include "../../../intermediate/item/Bmt.h"

class ArithMultiplyOperator : public ArithOperator {
private:
    Bmt* _bmt{};

public:
    ArithMultiplyOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank) : ArithOperator(
        x, y, l, taskTag, msgTagOffset, clientRank) {
    }

    ArithMultiplyOperator *execute() override;

    [[nodiscard]] static int tagStride(int width);

    ArithMultiplyOperator *setBmt(Bmt *bmt);
};


#endif
