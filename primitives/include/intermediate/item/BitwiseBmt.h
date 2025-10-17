
#ifndef BITWISEBMT_H
#define BITWISEBMT_H
#include "./Bmt.h"
#include "../../utils/Math.h"
#include "utils/Log.h"

class BitwiseBmt : public Bmt {
public:
    [[nodiscard]] BitwiseBmt extract(int i) const {
        BitwiseBmt b;
        b._a = Math::getBit(_a, i);
        b._b = Math::getBit(_b, i);
        b._c = Math::getBit(_c, i);
        return b;
    }

    static BitwiseBmt extract(std::vector<BitwiseBmt> &bmts, int i, int width) {
        int idx = i * width / 64;
        int offset = i % 64 * width;
        int64_t mask = (1ll << width) - 1;
        BitwiseBmt b;
        b._a = (bmts[idx]._a & (mask << offset)) >> offset;
        b._b = (bmts[idx]._b & (mask << offset)) >> offset;
        b._c = (bmts[idx]._c & (mask << offset)) >> offset;
        return b;
    }
};


#endif
