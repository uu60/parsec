
#ifndef BMTBATCHGENERATOR_H
#define BMTBATCHGENERATOR_H
#include "parsecdb/base/AbstractBatchOperator.h"
#include "parsecdb/intermediate/AbstractBmtBatchGenerator.h"
#include "parsecdb/intermediate/item/Bmt.h"

namespace parsecdb {

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

} // namespace parsecdb

#endif
