
#include "base/SecureOperator.h"

#include "conf/Conf.h"
#include "ot/RandOtBatchOperator.h"
#include "ot/RandOtOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

int SecureOperator::buildTag(int msgTag) const {
    int bits = 32 - Conf::TASK_TAG_BITS;
    return (static_cast<unsigned int>(_taskTag) << bits) | static_cast<unsigned int>(msgTag & ((1 << bits) - 1));
}


int64_t SecureOperator::ring(int64_t raw) const {
    return Math::ring(raw, _width);
}
