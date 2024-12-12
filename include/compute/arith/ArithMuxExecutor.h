//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "../ArithExecutor.h"

class ArithMuxExecutor : public ArithExecutor {
private:
    bool _cond_i{};

public:
    ArithMuxExecutor(int64_t x, int64_t y, bool c, int l, int16_t objTag, int16_t msgTagOffset, int clientRank);

    ArithMuxExecutor *execute() override;

    [[nodiscard]] std::string className() const override;
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
