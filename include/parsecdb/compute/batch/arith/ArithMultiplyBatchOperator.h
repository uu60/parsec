
#ifndef ARITHMULTIPLYBATCHOPERATOR_H
#define ARITHMULTIPLYBATCHOPERATOR_H

#include "parsecdb/compute/batch/arith/ArithBatchOperator.h"
#include "parsecdb/intermediate/item/Bmt.h"

namespace parsecdb {

class ArithMultiplyBatchOperator : public ArithBatchOperator {
  public:
    ArithMultiplyBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag,
                               int msgTagOffset, int clientRank);

    ArithMultiplyBatchOperator* execute() override;

    ArithMultiplyBatchOperator* reconstruct(int clientRank) override;

    [[nodiscard]] static int tagStride(int width);
};

} // namespace parsecdb

#endif
