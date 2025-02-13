//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef BOOLLESSTHANEXECUTOR_H
#define BOOLLESSTHANEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolLessExecutor : public BoolExecutor {
private:
    // BitwiseBmt *_bmt{};
    std::vector<BitwiseBmt> *_bmts{};


public:
    // reverse x and y to obey less than logic
    BoolLessExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank) : BoolExecutor(y, x, l, taskTag, msgTagOffset, clientRank) {};

    BoolLessExecutor *execute() override;

    BoolLessExecutor *setBmts(std::vector<BitwiseBmt> *bmts);

    static int16_t msgTagCount(int width);

    static int bmtCount(int width);

private:
    int64_t shiftGreater(int64_t in, int r) const;

    void prepareBmts(std::vector<BitwiseBmt> &bmts);
};



#endif //BOOLLESSTHANEXECUTOR_H
