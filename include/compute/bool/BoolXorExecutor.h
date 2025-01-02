//
// Created by 杜建璋 on 2024/11/15.
//

#ifndef XOREXECUTOR_H
#define XOREXECUTOR_H
#include "./BoolExecutor.h"

class BoolXorExecutor : public BoolExecutor {
public:
    BoolXorExecutor(bool x, bool y, int l, int16_t taskTag, int16_t msgTagOffset,
                       int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    };

    BoolXorExecutor *execute() override;

    [[nodiscard]] static int16_t needsMsgTags();

    [[nodiscard]] std::string className() const override;
};



#endif //XOREXECUTOR_H
