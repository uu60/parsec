//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H
#include "./ArithExecutor.h"
#include <vector>

#include "../../intermediate/item/Bmt.h"

class ArithLessExecutor : public ArithExecutor {
private:
    std::vector<Bmt> *_bmts{};

public:
    ArithLessExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank);

    ArithLessExecutor *execute() override;

    ArithLessExecutor *reconstruct(int clientRank) override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t needMsgTags();

    static std::pair<int, int> needBmtsWithBits(int l);

    ArithLessExecutor *setBmts(std::vector<Bmt> *bmts);
};

#endif //MPC_PACKAGE_COMPAREEXECUTOR_H
