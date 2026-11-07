
#ifndef BITWISEBMTGENERATOR_H
#define BITWISEBMTGENERATOR_H
#include "parsec/base/AbstractSingleOperator.h"
#include "parsec/intermediate/AbstractBmtSingleGenerator.h"
#include "parsec/intermediate/item/BitwiseBmt.h"
#include "parsec/intermediate/item/Bmt.h"

namespace parsec {

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

} // namespace parsec

#endif
