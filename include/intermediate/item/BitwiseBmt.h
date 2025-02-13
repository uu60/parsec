//
// Created by 杜建璋 on 2025/2/4.
//

#ifndef BITWISEBMT_H
#define BITWISEBMT_H
#include "./Bmt.h"
#include "../../utils/Math.h"

// Just for distinguish
class BitwiseBmt : public Bmt {
public:
    [[nodiscard]] BitwiseBmt extract(int i) const {
        BitwiseBmt b;
        b._a = Math::getBit(_a, i);
        b._b = Math::getBit(_b, i);
        b._c = Math::getBit(_c, i);
        return b;
    }
};


#endif //BITWISEBMT_H
