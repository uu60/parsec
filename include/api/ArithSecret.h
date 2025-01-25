//
// Created by 杜建璋 on 2024/9/12.
//

#ifndef MPC_PACKAGE_INTSECRET_H
#define MPC_PACKAGE_INTSECRET_H

#include "./BitSecret.h"
#include <vector>

#include "Secret.h"

class ArithSecret : public Secret {
private:
    int64_t _data{};
    int _l{};
    int16_t _taskTag{};

public:
    ArithSecret(int64_t x, int l, int16_t taskTag);

    ArithSecret task(int16_t taskTag) const;

    ArithSecret share(int clientRank) const;

    [[nodiscard]] ArithSecret add(ArithSecret yi) const;

    [[nodiscard]] ArithSecret mul(ArithSecret yi) const;

    ArithSecret reconstruct(int clientRank) const;

    [[nodiscard]] ArithSecret mux(ArithSecret yi, BitSecret cond_i) const;

    // need 3 * _l BMTs
    [[nodiscard]] ArithSecret boolean() const;

    [[nodiscard]] BitSecret lessThan(ArithSecret yi) const;

    [[nodiscard]] int64_t get() const;
};


#endif //MPC_PACKAGE_INTSECRET_H
