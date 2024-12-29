//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "./ArithExecutor.h"
#include "../../intermediate/item/Bmt.h"

class ArithMutexExecutor : public ArithExecutor {
private:
    bool _cond_i{};
    std::vector<Bmt> *_bmts{};

public:
    ArithMutexExecutor(int64_t x, int64_t y, bool c, int l, int16_t objTag, int16_t msgTagOffset, int clientRank);

    ArithMutexExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    static int64_t neededMsgTags();

    ArithMutexExecutor *setBmts(std::vector<Bmt> *bmts);
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
