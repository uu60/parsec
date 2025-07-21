//
// Created by 杜建璋 on 2025/1/8.
//

#ifndef BOOLSECRET_H
#define BOOLSECRET_H
#include <cstdint>

#include "./BitSecret.h"
#include "./Secret.h"

class BoolSecret : public Secret {
public:
    int64_t _data{};
    int _width{};
    int _taskTag{};
    int _currentMsgTag{};

public:
    BoolSecret();

    BoolSecret(int64_t x, int l, int taskTag, int msgTagOffset);

    BoolSecret task(int taskTag) const;

    BoolSecret msg(int msgTagOffset) const;

    BoolSecret share(int clientRank) const;

    [[nodiscard]] BoolSecret xor_(BoolSecret yi) const;

    [[nodiscard]] BoolSecret and_(BoolSecret yi) const;

    BoolSecret reconstruct(int clientRank) const;

    [[nodiscard]] BoolSecret mux(BoolSecret yi, BitSecret cond_i) const;

    [[nodiscard]] BoolSecret arithmetic() const;

    [[nodiscard]] BitSecret lessThan(BoolSecret yi) const;

    BitSecret getBit(int n) const;
};



#endif //BOOLSECRET_H
