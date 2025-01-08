//
// Created by 杜建璋 on 2024/12/2.
//

#ifndef TOARITHEXECUTOR_H
#define TOARITHEXECUTOR_H
#include "./BoolExecutor.h"


class BoolToArithExecutor : public BoolExecutor {
public:
    // Temporarily lend zi for xi preparation in super constructor.
    BoolToArithExecutor(int64_t xi, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank) : BoolExecutor(
        xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
    }

    BoolToArithExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t needsMsgTags(int l);

    BoolToArithExecutor *reconstruct(int clientRank) override;
};



#endif //TOARITHEXECUTOR_H
