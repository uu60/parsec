//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include "../AbstractSecureExecutor.h"
#include <iostream>

#include "Bmt.h"

class BmtGenerator : public AbstractSecureExecutor {
private:
    explicit BmtGenerator();

public:
    Bmt _bmt{};
    int64_t _ui{};
    int64_t _vi{};

    BmtGenerator *execute() override;

    BmtGenerator(const BmtGenerator &) = delete;

    BmtGenerator &operator=(const BmtGenerator &) = delete;

    static BmtGenerator &getInstance();

private:
    void generateRandomAB();

    void computeU();

    void computeV();

    void computeMix(int sender, int64_t &mix);

    void computeC();

    [[nodiscard]] int64_t corr(int i, int64_t x) const;

public:
    BmtGenerator *reconstruct(int clientRank) override;

protected:
    [[nodiscard]] std::string className() const override;
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
