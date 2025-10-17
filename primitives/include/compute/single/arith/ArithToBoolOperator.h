
#ifndef BOOLCONVERTEXECUTOR_H
#define BOOLCONVERTEXECUTOR_H
#include "./ArithOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithToBoolOperator : public ArithOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    ArithToBoolOperator(int64_t xi, int l, int taskTag, int msgTagOffset, int clientRank) : ArithOperator(
        xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    ArithToBoolOperator *execute() override;

    [[nodiscard]] static int tagStride(int width);

    ArithToBoolOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    ArithToBoolOperator *reconstruct(int clientRank) override;

    static int bmtCount(int width);

private:
    void prepareBmts(BitwiseBmt &b0, BitwiseBmt &b1, BitwiseBmt &b2) const;
};


#endif
