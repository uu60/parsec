//
// Created by 杜建璋 on 2024/12/1.
//

#ifndef BOOLCONVERTEXECUTOR_H
#define BOOLCONVERTEXECUTOR_H
#include "../../compute/ArithExecutor.h"
#include "../../intermediate/Bmt.h"

class ArithToBoolExecutor : public ArithExecutor {
private:
    std::vector<Bmt> *_bmts = nullptr;

public:
    // Temporarily lend zi for xi preparation in super constructor.
    ArithToBoolExecutor(int64_t xi, int l, int16_t objTag, int16_t msgTagOffset, int clientRank) : ArithExecutor(
        xi, l, objTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    ArithToBoolExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t neededMsgTags();

    ArithToBoolExecutor *setBmts(std::vector<Bmt> *bmts);
};


#endif //BOOLCONVERTEXECUTOR_H
