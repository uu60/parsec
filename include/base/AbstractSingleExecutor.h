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
    // unreconstructed share
    int64_t _zi{};

    AbstractSingleExecutor(int width, int16_t taskTag, int16_t msgTagOffset)
        : AbstractSecureExecutor(width, taskTag, msgTagOffset) {
    }
};



#endif //ABSTRACTSINGLEEXECUTOR_H
