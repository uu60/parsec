//
// Created by 杜建璋 on 2024/7/7.
//

#include "base/AbstractSecureExecutor.h"

#include "conf/Conf.h"
#include "ot/RandOtBatchExecutor.h"
#include "ot/RandOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

int AbstractSecureExecutor::buildTag(int msgTag) const {
    int bits = 32 - Conf::TASK_TAG_BITS;
    return (static_cast<unsigned int>(_taskTag) << bits) | static_cast<unsigned int>(msgTag & ((1 << bits) - 1));
}


int64_t AbstractSecureExecutor::ring(int64_t raw) const {
    return Math::ring(raw, _width);
}
