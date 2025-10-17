
#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H
#include "./ArithOperator.h"
#include <vector>

#include "../../../intermediate/item/BitwiseBmt.h"

class ArithLessOperator : public ArithOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    ArithLessOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank);

    ArithLessOperator *execute() override;

    ArithLessOperator *reconstruct(int clientRank) override;

    [[nodiscard]] static int tagStride(int l);

    static int bmtCount(int width);

    ArithLessOperator *setBmts(std::vector<BitwiseBmt> *bmts);
};

#endif
