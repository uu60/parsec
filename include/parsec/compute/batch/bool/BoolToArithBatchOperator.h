
#ifndef BOOLTOARITHBATCHOPERATOR_H
#define BOOLTOARITHBATCHOPERATOR_H
#include "parsec/compute/batch/bool/BoolBatchOperator.h"

namespace parsec {

class BoolToArithBatchOperator : public BoolBatchOperator {
  public:
    BoolToArithBatchOperator(std::vector<int64_t>* xs, int width, int taskTag, int msgTagOffset, int clientRank)
        : BoolBatchOperator(xs, nullptr, width, taskTag, msgTagOffset, clientRank) {};

    BoolToArithBatchOperator* execute() override;

    static int tagStride();
};

} // namespace parsec

#endif
