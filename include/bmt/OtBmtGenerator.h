//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#define MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
#include "../SecureExecutor.h"
#include "./AbstractTripleGenerator.h"
#include <iostream>

template<typename T>
class OtBmtGenerator : public AbstractTripleGenerator<T> {
public:
    const int _mask = (1 << this->_l) - 1;

public:
    explicit OtBmtGenerator();

    OtBmtGenerator *execute(bool dummy) override;

private:
    void generateRandomAB();

    void computeU();

    void computeV();

    void computeMix(int sender, T &mix);

    void computeC();

    [[nodiscard]] T corr(int i, T x) const;

protected:
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_RSAOTTRIPLEGENERATOR_H
