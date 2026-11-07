
#ifndef ARITHLESSBATCHOPERATOR_H
#define ARITHLESSBATCHOPERATOR_H

#include "parsecdb/compute/batch/arith/ArithBatchOperator.h"
#include "parsecdb/intermediate/item/BitwiseBmt.h"

namespace parsecdb {

class ArithLessBatchOperator : public ArithBatchOperator {
  public:
    inline static std::atomic_int64_t _totalTime = 0;

  public:
    ArithLessBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset,
                           int clientRank);

    ~ArithLessBatchOperator() override = default;

    ArithLessBatchOperator* execute() override;

    ArithLessBatchOperator* reconstruct(int clientRank) override;

    static int tagStride(int width);
};

} // namespace parsecdb

#endif
