//
// Created by 杜建璋 on 2024/9/12.
//

#ifndef MPC_PACKAGE_INTSECRET_H
#define MPC_PACKAGE_INTSECRET_H

#include "../utils/Comm.h"
#include "./BitSecret.h"
#include <vector>

#include "../bmt/BMT.h"

class IntSecret {
private:
    int64_t _data{};
    int _l{};
public:
    IntSecret(int64_t x, int l);

    IntSecret arithShare() const;

    IntSecret boolShare() const;

    [[nodiscard]] IntSecret add(IntSecret yi) const;

    [[nodiscard]] IntSecret mul(IntSecret yi, BMT bmt) const;

    IntSecret arithReconstruct() const;

    IntSecret boolReconstruct() const;

    [[nodiscard]] IntSecret mux(IntSecret yi, BitSecret cond_i, BMT bmt0, BMT bmt1) const;

    // needs 3 * _l BMTs
    [[nodiscard]] IntSecret boolean(std::vector<BMT> bmts) const;

    [[nodiscard]] IntSecret arithmetic() const;

    [[nodiscard]] BitSecret arithLessThan(IntSecret yi) const;

    [[nodiscard]] int64_t get() const;
};


#endif //MPC_PACKAGE_INTSECRET_H
