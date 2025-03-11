//
// Created by 杜建璋 on 2024/9/13.
//

#ifndef MPC_PACKAGE_BITSECRET_H
#define MPC_PACKAGE_BITSECRET_H
#include <cstdint>

#include "./Secret.h"

class BitSecret : public Secret {
public:
    bool _data{};
    int _taskTag{};
public:
    explicit BitSecret(bool x, int taskTag);

    BitSecret task(int taskTag);

    [[nodiscard]] BitSecret share(int clientRank) const;

    [[nodiscard]] BitSecret lessThan(BitSecret yi) const;

    [[nodiscard]] BitSecret not_() const;

    [[nodiscard]] BitSecret xor_(BitSecret yi) const;

    [[nodiscard]] BitSecret and_(BitSecret yi) const;

    [[nodiscard]] BitSecret or_(BitSecret yi) const;

    [[nodiscard]] BitSecret mux(BitSecret yi, BitSecret cond_i) const;

    BitSecret reconstruct(int clientRank) const;

    [[nodiscard]] bool get() const;
};


#endif //MPC_PACKAGE_BITSECRET_H
