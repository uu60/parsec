
#ifndef MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H

#include "parsec/compute/single/arith/ArithOperator.h"

namespace parsec {

class ArithAddOperator : public ArithOperator {
  public:
    ArithAddOperator(int64_t x, int64_t y, int width, int taskTag, int msgTagOffset, int clientRank)
        : ArithOperator(x, y, width, taskTag, msgTagOffset, clientRank) {}

    ArithAddOperator* execute() override;
};

} // namespace parsec

#endif
