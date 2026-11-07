
#include "parsec/base/SecureOperator.h"

#include "parsec/conf/Conf.h"
#include "parsec/ot/RandOtBatchOperator.h"
#include "parsec/ot/RandOtOperator.h"
#include "parsec/parallel/ThreadPoolSupport.h"
#include "parsec/utils/Math.h"

namespace parsec {

int SecureOperator::buildTag(int msgTag) const {
    int bits = 32 - Conf::TASK_TAG_BITS;
    return (static_cast<unsigned int>(_taskTag) << bits) | static_cast<unsigned int>(msgTag & ((1 << bits) - 1));
}

int64_t SecureOperator::ring(int64_t raw) const { return Math::ring(raw, _width); }

} // namespace parsec
