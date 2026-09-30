
#ifndef ARITHMULTIPLYBATCHOPERATOR_H
#define ARITHMULTIPLYBATCHOPERATOR_H

#include "./ArithBatchOperator.h"
#include "../../../intermediate/item/Bmt.h"

class ArithMultiplyBatchOperator : public ArithBatchOperator {
private:
    std::vector<Bmt> *_bmts{};

public:
    ArithMultiplyBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                               int width, int taskTag, int msgTagOffset, int clientRank);

    ArithMultiplyBatchOperator *execute() override;

    ArithMultiplyBatchOperator *reconstruct(int clientRank) override;

    ArithMultiplyBatchOperator *setBmts(std::vector<Bmt> *bmts);

    [[nodiscard]] static int bmtCount(int num);

    [[nodiscard]] static int tagStride(int width);
};

#endif
