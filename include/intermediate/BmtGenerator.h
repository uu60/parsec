//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include <iostream>

#include "./item/Bmt.h"
#include "../base/AbstractSingleExecutor.h"

class BmtGenerator : public AbstractSingleExecutor {
public:
    explicit BmtGenerator(int l, int16_t taskTag, int16_t msgTagOffset) : AbstractSingleExecutor(
        l, taskTag, msgTagOffset) {
    };

    Bmt _bmt{};
    int64_t _ui{};
    int64_t _vi{};

    BmtGenerator *execute() override;

protected:
    virtual void generateRandomAB();

    virtual void computeMix(int sender);

    virtual void computeC();

    virtual int64_t corr(int i, int64_t x) const;

public:
    BmtGenerator *reconstruct(int clientRank) override;

    static int16_t msgTagCount(int width);
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
