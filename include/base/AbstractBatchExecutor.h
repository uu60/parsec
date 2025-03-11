//
// Created by 杜建璋 on 2025/1/30.
//

#ifndef ABSTRACTBATCHEXECUTOR_H
#define ABSTRACTBATCHEXECUTOR_H
#include "./AbstractSecureExecutor.h"


class AbstractBatchExecutor : public AbstractSecureExecutor {
public:
    std::vector<int64_t> _results{};
    std::vector<int64_t> _xis{};
    std::vector<int64_t> _yis{};
    std::vector<int64_t> _zis{};

    AbstractBatchExecutor(int width, int taskTag, int msgTagOffset)
        : AbstractSecureExecutor(width, taskTag, msgTagOffset) {
    }
};



#endif //ABSTRACTBATCHEXECUTOR_H
