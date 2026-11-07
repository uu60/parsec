
#ifndef BITWISEBMTGENERATOR_H
#define BITWISEBMTGENERATOR_H
#include "parsecdb/base/AbstractSingleOperator.h"
#include "parsecdb/intermediate/AbstractBmtSingleGenerator.h"
#include "parsecdb/intermediate/item/BitwiseBmt.h"
#include "parsecdb/intermediate/item/Bmt.h"

namespace parsecdb {

class BitwiseBmtGenerator : public AbstractBmtSingleGenerator<BitwiseBmt> {
  public:
    inline static std::atomic_int64_t _totalTime = 0;

    explicit BitwiseBmtGenerator(int width, int taskTag, int msgTagOffset)
        : AbstractBmtSingleGenerator(width, taskTag, msgTagOffset) {};

    BitwiseBmtGenerator* execute() override;

  private:
    void generateRandomAB();

    void computeMix(int sender);

    void computeC();

    [[nodiscard]] int64_t corr(int i, int64_t x) const;

  public:
    SecureOperator* reconstruct(int clientRank) override;

    static int tagStride(int width);
};

} // namespace parsecdb

#endif
