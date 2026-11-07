
#include "parsecdb/base/SecureOperator.h"

#include "parsecdb/conf/Conf.h"
#include "parsecdb/ot/RandOtBatchOperator.h"
#include "parsecdb/ot/RandOtOperator.h"
#include "parsecdb/parallel/ThreadPoolSupport.h"
#include "parsecdb/utils/Math.h"

namespace parsecdb {

int SecureOperator::buildTag(int msgTag) const {
    int bits = 32 - Conf::TASK_TAG_BITS;
    return (static_cast<unsigned int>(_taskTag) << bits) | static_cast<unsigned int>(msgTag & ((1 << bits) - 1));
}

int64_t SecureOperator::ring(int64_t raw) const { return Math::ring(raw, _width); }

} // namespace parsecdb
