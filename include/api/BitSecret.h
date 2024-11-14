//
// Created by 杜建璋 on 2024/9/13.
//

#ifndef MPC_PACKAGE_BITSECRET_H
#define MPC_PACKAGE_BITSECRET_H

#include "./Secret.h"

class BitSecret : public Secret {
private:
    bool _data{};
public:
    explicit BitSecret(bool x);

    BitSecret share() const;

    [[nodiscard]] BitSecret not_() const;

    [[nodiscard]] BitSecret xor_(BitSecret yi) const;

    [[nodiscard]] BitSecret and_(BitSecret yi, bool ai, bool bi, bool ci) const;

    [[nodiscard]] BitSecret or_(BitSecret yi, bool ai, bool bi, bool ci) const;

    [[nodiscard]] BitSecret mux(BitSecret yi, BitSecret cond_i, bool ai, bool bi, bool ci) const;

    BitSecret reconstruct() const;

    [[nodiscard]] bool get() const;
};


#endif //MPC_PACKAGE_BITSECRET_H
