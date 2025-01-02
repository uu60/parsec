//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef BOOLLESSTHANEXECUTOR_H
#define BOOLLESSTHANEXECUTOR_H
#include "./BoolExecutor.h"

class BoolLessThanExecutor : public BoolExecutor {
public:
    bool _sign{};

    BoolLessThanExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset, int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {};

    BoolLessThanExecutor *execute() override;

    BoolLessThanExecutor *reconstruct(int clientRank) override;

    std::string className() const override;
};



#endif //BOOLLESSTHANEXECUTOR_H
