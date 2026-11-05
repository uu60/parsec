
#ifndef BITWISEBMT_H
#define BITWISEBMT_H
#include "./Bmt.h"
#include "utils/Math.h"
#include "utils/Log.h"

#include <cstdint>
#include <stdexcept>

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
        if (i < 0 || width <= 0 || width > 64) {
            throw std::invalid_argument("BitwiseBmt::extract received an invalid index or width");
        }

        const uint64_t bitIndex = static_cast<uint64_t>(i) * static_cast<uint64_t>(width);
        const size_t idx = bitIndex / 64;
        const int offset = static_cast<int>(bitIndex % 64);
        if (idx >= bmts.size() || offset + width > 64) {
            throw std::out_of_range("BitwiseBmt::extract field crosses a storage boundary");
        }

        const uint64_t mask = width == 64 ? ~uint64_t{0} : (uint64_t{1} << width) - 1;
        BitwiseBmt b;
        b._a = static_cast<int64_t>((static_cast<uint64_t>(bmts[idx]._a) >> offset) & mask);
        b._b = static_cast<int64_t>((static_cast<uint64_t>(bmts[idx]._b) >> offset) & mask);
        b._c = static_cast<int64_t>((static_cast<uint64_t>(bmts[idx]._c) >> offset) & mask);
        return b;
    }
};


#endif
