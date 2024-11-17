//
// Created by 杜建璋 on 2024/9/13.
//

#ifndef MPC_PACKAGE_BITSECRET_H
#define MPC_PACKAGE_BITSECRET_H
#include "../bmt/BMT.h"

class BitSecret {
private:
    bool _data{};
public:
    explicit BitSecret(bool x);

    [[nodiscard]] BitSecret share() const;

    [[nodiscard]] BitSecret not_() const;

    [[nodiscard]] BitSecret xor_(BitSecret yi) const;

    [[nodiscard]] BitSecret and_(BitSecret yi, BMT bmt) const;

    [[nodiscard]] BitSecret or_(BitSecret yi, BMT bmt) const;

    [[nodiscard]] BitSecret mux(BitSecret yi, BitSecret cond_i, BMT bmt0, BMT bmt1) const;

    BitSecret reconstruct() const;

    [[nodiscard]] bool get() const;
};


#endif //MPC_PACKAGE_BITSECRET_H
