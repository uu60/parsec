//
// Created by 杜建璋 on 2024/9/13.
//

#ifndef MPC_PACKAGE_BITSECRET_H
#define MPC_PACKAGE_BITSECRET_H
#include <cstdint>

class BitSecret {
private:
    bool _data{};
    int32_t _objTag{};
public:
    explicit BitSecret(bool x, int32_t objTag);

    [[nodiscard]] BitSecret share() const;

    [[nodiscard]] BitSecret not_() const;

    [[nodiscard]] BitSecret xor_(BitSecret yi) const;

    [[nodiscard]] BitSecret and_(BitSecret yi) const;

    [[nodiscard]] BitSecret or_(BitSecret yi) const;

    [[nodiscard]] BitSecret mux(BitSecret yi, BitSecret cond_i) const;

    BitSecret reconstruct(int clientRank) const;

    [[nodiscard]] bool get() const;
};


#endif //MPC_PACKAGE_BITSECRET_H
