
#ifndef MPC_PACKAGE_INTSECRET_H
#define MPC_PACKAGE_INTSECRET_H

#include "./BitSecret.h"
#include <vector>

#include "Secret.h"

class ArithSecret : public Secret {
public:
    int64_t _data{};
    int _width{};
    int _taskTag{};
    int _currentMsgTag{};

public:
    ArithSecret();

    ArithSecret(int64_t x, int l, int taskTag);

    ArithSecret(int64_t x, int l, int taskTag, int msgTagOffset);

    ArithSecret task(int taskTag) const;

    ArithSecret msg(int msgTagOffset) const;

    ArithSecret share(int clientRank) const;

    [[nodiscard]] ArithSecret add(ArithSecret yi) const;

    [[nodiscard]] ArithSecret mul(ArithSecret yi) const;

    ArithSecret reconstruct(int clientRank) const;

    [[nodiscard]] ArithSecret mux(ArithSecret yi, BitSecret cond_i) const;

    [[nodiscard]] ArithSecret boolean() const;

    [[nodiscard]] BitSecret lessThan(ArithSecret yi) const;

    BitSecret getBit(int n) const;
};


#endif
