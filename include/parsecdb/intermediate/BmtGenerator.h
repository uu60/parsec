
#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include <iostream>

#include "parsecdb/base/AbstractSingleOperator.h"
#include "parsecdb/intermediate/AbstractBmtSingleGenerator.h"
#include "parsecdb/intermediate/item/Bmt.h"

namespace parsecdb {

class BmtGenerator : public AbstractBmtSingleGenerator<Bmt> {
  public:
    explicit BmtGenerator(int width, int taskTag, int msgTagOffset)
        : AbstractBmtSingleGenerator(width, taskTag, msgTagOffset) {};

    BmtGenerator* execute() override;

  protected:
    virtual void generateRandomAB();

    virtual void computeMix(int sender);

    virtual void computeC();

    virtual int64_t corr(int i, int64_t x) const;

  public:
    BmtGenerator* reconstruct(int clientRank) override;

    static int tagStride(int width);
};

} // namespace parsecdb

#endif
