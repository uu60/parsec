//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include "../AbstractSecureExecutor.h"
#include <iostream>

#include "Bmt.h"

class OtBmtGenerator : public AbstractSecureExecutor {
private:
    explicit OtBmtGenerator();

public:
    Bmt _bmt{};
    int64_t _ui{};
    int64_t _vi{};

    OtBmtGenerator *execute() override;

    OtBmtGenerator(const OtBmtGenerator &) = delete;

    OtBmtGenerator &operator=(const OtBmtGenerator &) = delete;

    static OtBmtGenerator &getInstance();

private:
    void generateRandomAB();

    void computeU();

    void computeV();

    void computeMix(int sender, int64_t &mix);

    void computeC();

    [[nodiscard]] int64_t corr(int i, int64_t x) const;

public:
    AbstractSecureExecutor *reconstruct(int clientRank) override;

protected:
    [[nodiscard]] std::string className() const override;
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
