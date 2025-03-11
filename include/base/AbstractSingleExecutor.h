//
// Created by 杜建璋 on 2025/1/30.
//

#ifndef ABSTRACTSINGLEEXECUTOR_H
#define ABSTRACTSINGLEEXECUTOR_H
#include "./AbstractSecureExecutor.h"


class AbstractSingleExecutor : public AbstractSecureExecutor {
public:
    // result
    int64_t _result{};
    int64_t _xi{};
    int64_t _yi{};
    // unreconstructed share
    int64_t _zi{};

    AbstractSingleExecutor(int width, int taskTag, int msgTagOffset)
        : AbstractSecureExecutor(width, taskTag, msgTagOffset) {
    }
};



#endif //ABSTRACTSINGLEEXECUTOR_H
