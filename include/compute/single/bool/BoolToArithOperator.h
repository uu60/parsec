//
// Created by 杜建璋 on 2024/12/2.
//

#ifndef TOARITHEXECUTOR_H
#define TOARITHEXECUTOR_H
#include "./BoolOperator.h"


class BoolToArithOperator : public BoolOperator {
public:
    // Temporarily lend zi for xi preparation in super constructor.
    BoolToArithOperator(int64_t xi, int l, int taskTag, int msgTagOffset, int clientRank) : BoolOperator(
        xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    BoolToArithOperator *execute() override;

    [[nodiscard]] static int msgTagCount(int width);

    BoolToArithOperator *reconstruct(int clientRank) override;
};



#endif //TOARITHEXECUTOR_H
