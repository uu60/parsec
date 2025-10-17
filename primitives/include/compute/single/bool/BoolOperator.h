
#ifndef INTBOOLEXECUTOR_H
#define INTBOOLEXECUTOR_H
#include "../../../base/AbstractSingleOperator.h"

class BoolOperator : public AbstractSingleOperator {
public:
    BoolOperator(int64_t z, int l, int taskTag, int msgTagOffset, int clientRank);

    BoolOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank);

    BoolOperator *reconstruct(int clientRank) override;

    BoolOperator *execute() override;
};

#endif
