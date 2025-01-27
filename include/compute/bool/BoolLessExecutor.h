//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef BOOLLESSTHANEXECUTOR_H
#define BOOLLESSTHANEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../intermediate/item/Bmt.h"

class BoolLessExecutor : public BoolExecutor {
private:
    // Bmt *_bmt{};
    std::vector<Bmt> *_bmts{};

public:
    // reverse x and y to obey less than logic
    BoolLessExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank) : BoolExecutor(y, x, l, taskTag, msgTagOffset, clientRank) {};

    BoolLessExecutor *execute() override;

    std::string className() const override;

    static int needMsgTags();

    BoolLessExecutor *setBmts(std::vector<Bmt> *bmts);

    static std::pair<int, int> needBmtsAndBits(int l);

private:
    int64_t shiftGreater(int64_t in, int r) const;
};



#endif //BOOLLESSTHANEXECUTOR_H
