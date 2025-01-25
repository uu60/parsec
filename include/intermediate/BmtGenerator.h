//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include "../AbstractSecureExecutor.h"
#include <iostream>

#include "./item/Bmt.h"

class BmtGenerator : public AbstractSecureExecutor {
public:
    explicit BmtGenerator(int l, int16_t taskTag, int16_t msgTagOffset) : AbstractSecureExecutor(
        l, taskTag, msgTagOffset) {
    };

    Bmt _bmt{};
    int64_t _ui{};
    int64_t _vi{};

    BmtGenerator *execute() override;

private:
    void generateRandomAB();

    void computeU();

    void computeV();

    void computeMix(int sender, int64_t &mix);

    void computeC();

    [[nodiscard]] int64_t corr(int i, int64_t x) const;

public:
    BmtGenerator *reconstruct(int clientRank) override;

    static int16_t needMsgTags(int l);

protected:
    [[nodiscard]] std::string className() const override;
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
