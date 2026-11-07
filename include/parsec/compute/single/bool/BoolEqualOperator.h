
#ifndef BOOLEQUALEXECUTOR_H
#define BOOLEQUALEXECUTOR_H
#
#include "parsec/compute/single/bool/BoolOperator.h"
#include "parsec/intermediate/item/BitwiseBmt.h"

namespace parsec {

class BoolEqualOperator : public BoolOperator {
  private:
    BitwiseBmt* _bmt{};

  public:
    BoolEqualOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank)
        : BoolOperator(x, y, l, taskTag, msgTagOffset, clientRank) {}

    BoolEqualOperator* execute() override;

    BoolEqualOperator* setBmt(BitwiseBmt* bmt);
};

} // namespace parsec

#endif
