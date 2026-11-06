
#ifndef RANDOTBATCHEXECUTOR_H
#define RANDOTBATCHEXECUTOR_H
#include "./base/AbstractOtBatchOperator.h"

namespace parsec {

// INSECURE - do not use for protocol computations. Every transfer, in every call, is masked with the same single random
// OT prepared once in IntermediateDataSupport::prepareRot() (_sRot0 / _rRot0). The receiver knows one of the two masks,
// so the XOR of any two unchosen messages masked with the same pad leaks, and one known message reveals the others.
// Kept only for benchmarks and as a performance baseline; protocol code uses IknpOtBatchOperator.
class RandOtBatchOperator : public AbstractOtBatchOperator {
  public:
    inline static std::atomic_int64_t _totalTime = 0;
    bool _doBits{};
    std::vector<int64_t>* _choiceBits{};

  public:
    RandOtBatchOperator(int sender, std::vector<int64_t>* ms0, std::vector<int64_t>* ms1, std::vector<int>* choices,
                        int width, int taskTag, int msgTagOffset)
        : AbstractOtBatchOperator(sender, ms0, ms1, choices, width, taskTag, msgTagOffset) {}

    RandOtBatchOperator(int sender, std::vector<int64_t>* bits0, std::vector<int64_t>* bits1,
                        std::vector<int64_t>* choiceBits, int taskTag, int msgTagOffset);

    RandOtBatchOperator* execute() override;

    static int tagStride();

  private:
    void execute0();

    void executeForBits();

    void executeForBitsSingleTransfer();

    void executeForBitsAsync();
};

} // namespace parsec

#endif
