
#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include <iostream>

#include "../base/AbstractSingleOperator.h"
#include "./item/Bmt.h"
#include "AbstractBmtSingleGenerator.h"

namespace parsec {

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

} // namespace parsec

#endif
