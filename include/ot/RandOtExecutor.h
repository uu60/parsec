//
// Created by 杜建璋 on 2024/12/29.
//

#ifndef RANDOTEXECUTOR_H
#define RANDOTEXECUTOR_H
#include "./base/AbstractOtOperator.h"


class RandOtOperator : public AbstractOtOperator {
public:
    RandOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag,
                   int msgTagOffset) : AbstractOtOperator(sender, m0, m1, choice, l, taskTag, msgTagOffset) {
    }

    RandOtOperator *execute() override;

    static int msgTagCount(int width);
};

#endif //RANDOTEXECUTOR_H
