//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include <iostream>

#include "AbstractBmtSingleGenerator.h"
#include "./item/Bmt.h"
#include "../base/AbstractSingleExecutor.h"

class BmtGenerator : public AbstractBmtSingleGenerator<Bmt> {
public:
    explicit BmtGenerator(int width, int taskTag, int msgTagOffset) : AbstractBmtSingleGenerator(
        width, taskTag, msgTagOffset) {
    };

    BmtGenerator *execute() override;

protected:
    virtual void generateRandomAB();

    virtual void computeMix(int sender);

    virtual void computeC();

    virtual int64_t corr(int i, int64_t x) const;

public:
    BmtGenerator *reconstruct(int clientRank) override;

    static int msgTagCount(int width);
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
