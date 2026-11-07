
#ifndef TOARITHEXECUTOR_H
#define TOARITHEXECUTOR_H
#include "parsecdb/compute/single/bool/BoolOperator.h"

namespace parsecdb {

class BoolToArithOperator : public BoolOperator {
  public:
    BoolToArithOperator(int64_t xi, int l, int taskTag, int msgTagOffset, int clientRank)
        : BoolOperator(xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    BoolToArithOperator* execute() override;

    [[nodiscard]] static int tagStride(int width);

    BoolToArithOperator* reconstruct(int clientRank) override;
};

} // namespace parsecdb

#endif
