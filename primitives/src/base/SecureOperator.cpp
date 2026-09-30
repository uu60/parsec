
#include "base/SecureOperator.h"

#include "conf/Conf.h"
#include "ot/RandOtBatchOperator.h"
#include "ot/RandOtOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

int SecureOperator::buildTag(int msgTag) const {
    const int bits = 32 - Conf::TASK_TAG_BITS;
    const uint32_t msgMask = (uint32_t{1} << bits) - 1;
    const uint32_t taskMask = (uint32_t{1} << Conf::TASK_TAG_BITS) - 1;

    // Primitive code passes a logical task number in _taskTag, while the DB
    // layer historically passes an already encoded task prefix in msgTag (and
    // occasionally in _taskTag). Preserve both calling conventions. Dropping
    // the high bits from msgTag aliases DB traffic with task 0, which is
    // reserved by the Background BMT producer.
    const uint32_t rawTask = static_cast<uint32_t>(_taskTag);
    const uint32_t taskPrefix = rawTask <= taskMask
                                    ? rawTask << bits
                                    : rawTask & ~msgMask;
    const uint32_t rawMsg = static_cast<uint32_t>(msgTag);
    return static_cast<int>(taskPrefix | rawMsg);
}


int64_t SecureOperator::ring(int64_t raw) const {
    return Math::ring(raw, _width);
}
