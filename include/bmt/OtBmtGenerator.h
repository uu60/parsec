//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include "../SecureExecutor.h"
#include <iostream>

#include "BMT.h"

class OtBmtGenerator : public SecureExecutor {
public:
    BMT _bmt{};
    int64_t _ui{};
    int64_t _vi{};

    explicit OtBmtGenerator(int l);

    OtBmtGenerator *execute() override;

private:
    void generateRandomAB();

    void computeU();

    void computeV();

    void computeMix(int sender, int64_t &mix);

    void computeC();

    [[nodiscard]] int64_t corr(int i, int64_t x) const;

protected:
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
