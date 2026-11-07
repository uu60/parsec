
#ifndef BOOLBATCHEXECUTOR_H
#define BOOLBATCHEXECUTOR_H
#include "parsecdb/base/AbstractBatchOperator.h"

namespace parsecdb {

class BoolBatchOperator : public AbstractBatchOperator {
  public:
    BoolBatchOperator(std::vector<int64_t>& zs, int width, int taskTag, int msgTagOffset, int clientRank);

    BoolBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset,
                      int clientRank);

    BoolBatchOperator* reconstruct(int clientRank) override;

    BoolBatchOperator* execute() override;
};

} // namespace parsecdb

#endif
