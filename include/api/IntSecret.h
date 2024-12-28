//
// Created by 杜建璋 on 2024/9/12.
//

#ifndef MPC_PACKAGE_INTSECRET_H
#define MPC_PACKAGE_INTSECRET_H

#include "./BitSecret.h"
#include <vector>

class IntSecret {
private:
    int64_t _data{};
    int _l{};
    int16_t _objTag{};
public:
    IntSecret(int64_t x, int l, int16_t objTag);

    IntSecret arithShare(int clientRank) const;

    IntSecret boolShare(int clientRank) const;

    [[nodiscard]] IntSecret add(IntSecret yi) const;

    [[nodiscard]] IntSecret mul(IntSecret yi) const;

    [[nodiscard]] IntSecret xor_(IntSecret yi) const;

    [[nodiscard]] IntSecret and_(IntSecret yi) const;

    IntSecret arithReconstruct(int clientRank) const;

    IntSecret boolReconstruct(int clientRank) const;

    [[nodiscard]] IntSecret mux(IntSecret yi, BitSecret cond_i) const;

    // needs 3 * _l BMTs
    [[nodiscard]] IntSecret boolean() const;

    [[nodiscard]] IntSecret arithmetic() const;

    [[nodiscard]] BitSecret arithLessThan(IntSecret yi) const;

    [[nodiscard]] int64_t get() const;
};


#endif //MPC_PACKAGE_INTSECRET_H
