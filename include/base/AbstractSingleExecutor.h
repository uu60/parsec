//
// Created by 杜建璋 on 2025/1/30.
//

#ifndef ABSTRACTSINGLEEXECUTOR_H
#define ABSTRACTSINGLEEXECUTOR_H
#include "./SecureOperator.h"


class AbstractSingleOperator : public SecureOperator {
public:
    // result
    int64_t _result{};
    int64_t _xi{};
    int64_t _yi{};
    // unreconstructed share
    int64_t _zi{};

    AbstractSingleOperator(int width, int taskTag, int msgTagOffset)
        : SecureOperator(width, taskTag, msgTagOffset) {
    }
};



#endif //ABSTRACTSINGLEEXECUTOR_H
