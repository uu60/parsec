
#ifndef ARITHBATCHEXECUTOR_H
#define ARITHBATCHEXECUTOR_H
#include "../../../base/AbstractBatchOperator.h"


class ArithBatchOperator : public AbstractBatchOperator {
public:
    ArithBatchOperator(std::vector<int64_t>& zs, int width, int taskTag, int msgTagOffset, int clientRank);

    ArithBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width, int taskTag, int msgTagOffset, int clientRank);

    ArithBatchOperator *reconstruct(int clientRank) override;

    ArithBatchOperator *execute() override;
};



#endif
