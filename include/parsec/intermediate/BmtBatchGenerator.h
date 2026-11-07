
#ifndef BMTBATCHGENERATOR_H
#define BMTBATCHGENERATOR_H
#include "parsec/base/AbstractBatchOperator.h"
#include "parsec/intermediate/AbstractBmtBatchGenerator.h"
#include "parsec/intermediate/item/Bmt.h"

namespace parsec {

class BmtBatchGenerator : public AbstractBmtBatchGenerator<Bmt> {
  public:
    BmtBatchGenerator(int count, int l, int taskTag, int msgTagOffset);

    BmtBatchGenerator* execute() override;

  protected:
    virtual void generateRandomAB();

    virtual void computeMix(int sender);

    virtual void computeC();

    virtual int64_t corr(int bmtIdx, int round, int64_t x) const;

  public:
    BmtBatchGenerator* reconstruct(int clientRank) override;

    static int tagStride();
};

} // namespace parsec

#endif
