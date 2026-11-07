
#ifndef BOOLTOARITHBATCHOPERATOR_H
#define BOOLTOARITHBATCHOPERATOR_H
#include "parsecdb/compute/batch/bool/BoolBatchOperator.h"

namespace parsecdb {

class BoolToArithBatchOperator : public BoolBatchOperator {
  public:
    BoolToArithBatchOperator(std::vector<int64_t>* xs, int width, int taskTag, int msgTagOffset, int clientRank)
        : BoolBatchOperator(xs, nullptr, width, taskTag, msgTagOffset, clientRank) {};

    BoolToArithBatchOperator* execute() override;

    static int tagStride();
};

} // namespace parsecdb

#endif
