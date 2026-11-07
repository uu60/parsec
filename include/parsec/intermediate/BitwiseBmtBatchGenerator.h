
#ifndef BITWISEBMTBATCHGENERATOR_H
#define BITWISEBMTBATCHGENERATOR_H
#include "parsec/base/AbstractBatchOperator.h"
#include "parsec/intermediate/AbstractBmtBatchGenerator.h"
#include "parsec/intermediate/item/BitwiseBmt.h"

namespace parsec {

class BitwiseBmtBatchGenerator : public AbstractBmtBatchGenerator<BitwiseBmt> {
  public:
    inline static std::atomic_int64_t _totalTime = 0;
    int64_t _totalBits{};
    int _bc{};

    BitwiseBmtBatchGenerator(int count, int width, int taskTag, int msgTagOffset);

    BitwiseBmtBatchGenerator* execute() override;

  private:
    void generateRandomAB();

    void computeMix(int sender);

    void computeC();

    [[nodiscard]] int64_t corr(int bmtIdx, int64_t x) const;

  public:
    SecureOperator* reconstruct(int clientRank) override;

    static int tagStride();
};

} // namespace parsec

#endif
