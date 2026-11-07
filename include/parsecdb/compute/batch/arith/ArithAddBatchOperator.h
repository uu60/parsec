
#ifndef ARITHADDBATCHOPERATOR_H
#define ARITHADDBATCHOPERATOR_H
#include "parsecdb/compute/batch/arith/ArithBatchOperator.h"

namespace parsecdb {

class ArithAddBatchOperator : public ArithBatchOperator {
  public:
    ArithAddBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset,
                          int clientRank)
        : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {}

    ArithAddBatchOperator* execute() override;

    static int tagStride();
};

} // namespace parsecdb

#endif
