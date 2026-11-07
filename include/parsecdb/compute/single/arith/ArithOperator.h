
#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include <vector>

#include "parsecdb/base/AbstractSingleOperator.h"

namespace parsecdb {

class ArithOperator : public AbstractSingleOperator {
  public:
    ArithOperator(int64_t z, int width, int taskTag, int msgTagOffset, int clientRank);

    ArithOperator(int64_t x, int64_t y, int width, int taskTag, int msgTagOffset, int clientRank);

    ArithOperator* reconstruct(int clientRank) override;

    ArithOperator* execute() override;
};

} // namespace parsecdb

#endif
