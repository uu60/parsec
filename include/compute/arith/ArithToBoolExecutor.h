//
// Created by 杜建璋 on 2024/12/1.
//

#ifndef BOOLCONVERTEXECUTOR_H
#define BOOLCONVERTEXECUTOR_H
#include "./ArithExecutor.h"
#include "../../intermediate/item/Bmt.h"

class ArithToBoolExecutor : public ArithExecutor {
private:
    std::vector<Bmt> *_bmts{};

public:
    // Temporarily lend zi for xi preparation in super constructor.
    ArithToBoolExecutor(int64_t xi, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank) : ArithExecutor(
        xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    ArithToBoolExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t needMsgTags();

    static std::pair<int, int> needBmtsWithBits(int l);

    ArithToBoolExecutor *setBmts(std::vector<Bmt> *bmts);

    ArithToBoolExecutor *reconstruct(int clientRank) override;
};


#endif //BOOLCONVERTEXECUTOR_H
