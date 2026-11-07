
#ifndef BOOLEQUALEXECUTOR_H
#define BOOLEQUALEXECUTOR_H
#
#include "parsecdb/compute/single/bool/BoolOperator.h"
#include "parsecdb/intermediate/item/BitwiseBmt.h"

namespace parsecdb {

class BoolEqualOperator : public BoolOperator {
  private:
    BitwiseBmt* _bmt{};

  public:
    BoolEqualOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank)
        : BoolOperator(x, y, l, taskTag, msgTagOffset, clientRank) {}

    BoolEqualOperator* execute() override;

    BoolEqualOperator* setBmt(BitwiseBmt* bmt);
};

} // namespace parsecdb

#endif
