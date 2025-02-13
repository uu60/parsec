//
// Created by 杜建璋 on 2024/12/1.
//

#ifndef BOOLCONVERTEXECUTOR_H
#define BOOLCONVERTEXECUTOR_H
#include "./ArithExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class ArithToBoolExecutor : public ArithExecutor {
private:
    std::vector<BitwiseBmt> *_bmts{};
    // BitwiseBmt *_bmt0{};
    // BitwiseBmt *_bmt1{};
    // BitwiseBmt *_bmt2{};

public:
    // Temporarily lend zi for xi preparation in super constructor.
    ArithToBoolExecutor(int64_t xi, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank) : ArithExecutor(
        xi, l, taskTag, msgTagOffset, clientRank) {
        _xi = _zi;
        _zi = 0;
    }

    ArithToBoolExecutor *execute() override;

    [[nodiscard]] static int16_t msgTagCount(int l);

    ArithToBoolExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    ArithToBoolExecutor *reconstruct(int clientRank) override;

    static int bmtCount(int width);

private:
    void prepareBmts(BitwiseBmt &b0, BitwiseBmt &b1, BitwiseBmt &b2) const;
};


#endif //BOOLCONVERTEXECUTOR_H
