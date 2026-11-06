
#ifndef RANDOTEXECUTOR_H
#define RANDOTEXECUTOR_H
#include "./base/AbstractOtOperator.h"

namespace parsec {

// INSECURE - do not use for protocol computations. Every transfer, in every call, is masked with the same single random
// OT prepared once in IntermediateDataSupport::prepareRot() (_sRot0 / _rRot0). The receiver knows one of the two masks,
// so the XOR of any two unchosen messages masked with the same pad leaks, and one known message reveals the others.
// Kept only for benchmarks and as a performance baseline; protocol code uses IknpOtBatchOperator.
class RandOtOperator : public AbstractOtOperator {
  public:
    RandOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset)
        : AbstractOtOperator(sender, m0, m1, choice, l, taskTag, msgTagOffset) {}

    RandOtOperator* execute() override;

    static int tagStride(int width);
};

} // namespace parsec

#endif
