
#ifndef ARITHTOBOOLBATCHOPERATOR_H
#define ARITHTOBOOLBATCHOPERATOR_H
#include "parsec/compute/batch/arith/ArithBatchOperator.h"
#include "parsec/intermediate/item/BitwiseBmt.h"

namespace parsec {

class ArithToBoolBatchOperator : public ArithBatchOperator {
  public:
    inline static std::atomic_int64_t _totalTime = 0;

  public:
    ArithToBoolBatchOperator(std::vector<int64_t>* xs, int width, int taskTag, int msgTagOffset, int clientRank);

    ~ArithToBoolBatchOperator() override;

    ArithToBoolBatchOperator* execute() override;

    ArithToBoolBatchOperator* reconstruct(int clientRank) override;

    static int tagStride(int width);
};

} // namespace parsec

#endif
