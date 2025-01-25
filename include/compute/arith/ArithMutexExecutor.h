//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "./ArithExecutor.h"
#include "../../intermediate/item/Bmt.h"

class ArithMutexExecutor : public ArithExecutor {
private:
    int64_t _cond_i{};
    std::vector<Bmt> *_bmts{};

public:
    ArithMutexExecutor(int64_t x, int64_t y, bool c, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithMutexExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    static int16_t needMsgTags(int l);

    ArithMutexExecutor *setBmts(std::vector<Bmt> *bmts);

    static std::pair<int, int> needBmtsAndBits(int l);
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
