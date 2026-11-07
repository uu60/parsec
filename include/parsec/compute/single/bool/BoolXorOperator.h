
#ifndef XOREXECUTOR_H
#define XOREXECUTOR_H
#include "parsec/compute/single/bool/BoolOperator.h"

namespace parsec {

class BoolXorOperator : public BoolOperator {
  public:
    BoolXorOperator(bool x, bool y, int l, int taskTag, int msgTagOffset, int clientRank)
        : BoolOperator(x, y, l, taskTag, msgTagOffset, clientRank) {};

    BoolXorOperator* execute() override;

    [[nodiscard]] static int tagStride();
};

} // namespace parsec

#endif
