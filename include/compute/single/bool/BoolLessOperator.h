//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef BOOLLESSTHANEXECUTOR_H
#define BOOLLESSTHANEXECUTOR_H
#include "./BoolOperator.h"
#include "../../../intermediate/item/BitwiseBmt.h"

class BoolLessOperator : public BoolOperator {
private:
    // BitwiseBmt *_bmt{};
    std::vector<BitwiseBmt> *_bmts{};

public:
    inline static std::atomic_int64_t _totalTime = 0;

public:
    // reverse x and y to obey less than logic
    BoolLessOperator(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset, int clientRank) : BoolOperator(y, x, l, taskTag, msgTagOffset, clientRank) {};

    BoolLessOperator *execute() override;

    BoolLessOperator *setBmts(std::vector<BitwiseBmt> *bmts);

    static int tagStride(int width);

    static int bmtCount(int width);

private:
    int64_t shiftGreater(int64_t in, int r) const;

    bool prepareBmts(std::vector<BitwiseBmt> &bmts);
};



#endif //BOOLLESSTHANEXECUTOR_H
