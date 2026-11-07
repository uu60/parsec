
#ifndef ARITHMULTIPLYBATCHOPERATOR_H
#define ARITHMULTIPLYBATCHOPERATOR_H

#include "parsec/compute/batch/arith/ArithBatchOperator.h"
#include "parsec/intermediate/item/Bmt.h"

namespace parsec {

class ArithMultiplyBatchOperator : public ArithBatchOperator {
  public:
    ArithMultiplyBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag,
                               int msgTagOffset, int clientRank);

    ArithMultiplyBatchOperator* execute() override;

    ArithMultiplyBatchOperator* reconstruct(int clientRank) override;

    [[nodiscard]] static int tagStride(int width);
};

} // namespace parsec

#endif
