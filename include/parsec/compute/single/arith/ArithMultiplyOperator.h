
#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#include "parsec/compute/single/arith/ArithOperator.h"
#include "parsec/intermediate/item/Bmt.h"

namespace parsec {

class ArithMultiplyOperator : public ArithOperator {
  private:
    Bmt* _bmt{};

  public:
    ArithMultiplyOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank)
        : ArithOperator(x, y, l, taskTag, msgTagOffset, clientRank) {}

    ArithMultiplyOperator* execute() override;

    [[nodiscard]] static int tagStride(int width);

    ArithMultiplyOperator* setBmt(Bmt* bmt);
};

} // namespace parsec

#endif
