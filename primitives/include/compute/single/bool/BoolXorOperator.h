//
// Created by 杜建璋 on 2024/11/15.
//

#ifndef XOREXECUTOR_H
#define XOREXECUTOR_H
#include "./BoolOperator.h"

class BoolXorOperator : public BoolOperator {
public:
    BoolXorOperator(bool x, bool y, int l, int taskTag, int msgTagOffset,
                       int clientRank) : BoolOperator(x, y, l, taskTag, msgTagOffset, clientRank) {
    };

    BoolXorOperator *execute() override;

    [[nodiscard]] static int tagStride();
};



#endif //XOREXECUTOR_H
