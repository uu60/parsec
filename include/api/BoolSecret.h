//
// Created by 杜建璋 on 2025/1/8.
//

#ifndef BOOLSECRET_H
#define BOOLSECRET_H
#include <cstdint>

#include "BitSecret.h"
#include "Secret.h"

class BoolSecret : public Secret {
private:
    int64_t _data{};
    int _l{};
    int16_t _taskTag{};

public:
    BoolSecret(int64_t x, int l, int16_t taskTag);

    BoolSecret task(int16_t taskTag) const;

    BoolSecret share(int clientRank) const;

    [[nodiscard]] BoolSecret xor_(BoolSecret yi) const;

    [[nodiscard]] BoolSecret and_(BoolSecret yi) const;

    BoolSecret reconstruct(int clientRank) const;

    [[nodiscard]] BoolSecret mux(BoolSecret yi, BitSecret cond_i) const;

    [[nodiscard]] BoolSecret arithmetic() const;

    [[nodiscard]] BitSecret lessThan(BoolSecret yi) const;

    [[nodiscard]] int64_t get() const;
};



#endif //BOOLSECRET_H
