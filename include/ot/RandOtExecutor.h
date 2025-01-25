//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef RANDOTEXECUTOR_H
#define RANDOTEXECUTOR_H
#include "AbstractOtExecutor.h"


class RandOtExecutor : public AbstractOtExecutor {
public:
    RandOtExecutor(int sender, int64_t m0, int64_t m1, int choice, int l, int16_t taskTag, int16_t msgTagOffset) : AbstractOtExecutor(sender, m0, m1, choice, l, taskTag, msgTagOffset) {}

    RandOtExecutor *execute() override;

    static int16_t needMsgTags();

    std::string className() const override;
};

#endif //RANDOTEXECUTOR_H
