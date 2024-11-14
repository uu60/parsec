//
// Created by 杜建璋 on 2024/9/12.
//

#ifndef MPC_PACKAGE_INTSECRET_H
#define MPC_PACKAGE_INTSECRET_H

#include "./Secret.h"
#include "../utils/Comm.h"
#include "./BitSecret.h"
#include <vector>

template<typename T>
class IntSecret : public Secret {
private:
    T _data{};
public:
    explicit IntSecret(T x);

    IntSecret arithShare() const;

    IntSecret boolShare() const;

    [[nodiscard]] IntSecret add(IntSecret yi) const;

    [[nodiscard]] IntSecret mul(IntSecret yi, T ai, T bi, T ci) const;

    IntSecret arithReconstruct() const;

    IntSecret boolReconstruct() const;

    [[nodiscard]] IntSecret mux(IntSecret yi, BitSecret cond_i, T ai, T bi, T ci) const;

    [[nodiscard]] IntSecret boolean() const;

    [[nodiscard]] IntSecret arithmetic() const;

    [[nodiscard]] BitSecret arithLessThan(IntSecret yi) const;

    [[nodiscard]] T get() const;
};


#endif //MPC_PACKAGE_INTSECRET_H
