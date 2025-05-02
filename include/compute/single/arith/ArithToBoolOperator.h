//
// Created by 杜建璋 on 2024/12/1.
//

#ifndef BOOLCONVERTEXECUTOR_H
#define BOOLCONVERTEXECUTOR_H
#include "./ArithOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithToBoolOperator : public ArithOperator {
private:
    std::vector<BitwiseBmt> *_bmts{};

public:
    // Temporarily lend zi for xi preparation in super constructor.
    ArithToBoolOperator(int64_t xi, int l, int taskTag, int msgTagOffset, int clientRank) : ArithOperator(
        xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    ArithToBoolOperator *execute() override;

    [[nodiscard]] static int msgTagCount(int l);

    ArithToBoolOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    ArithToBoolOperator *reconstruct(int clientRank) override;

    static int bmtCount(int width);

private:
    void prepareBmts(BitwiseBmt &b0, BitwiseBmt &b1, BitwiseBmt &b2) const;
};


#endif //BOOLCONVERTEXECUTOR_H
