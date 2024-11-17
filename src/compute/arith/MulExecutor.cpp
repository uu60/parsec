//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/arith/MulExecutor.h"
#include "utils/Comm.h"
#include "utils/Math.h"
#include "utils/Log.h"

MulExecutor::MulExecutor(int64_t x, int64_t y, int l, bool local) : ArithExecutor(x, y, l, local) {}

MulExecutor* MulExecutor::execute() {
    // process
    if (Comm::isServer()) {
        int64_t ei = ring(_xi - _bmt._a);
        int64_t fi = ring(_yi - _bmt._b);
        int64_t eo, fo;
        Comm::sexch(&ei, &eo);
        Comm::sexch(&fi, &fo);
        int64_t e = ring(ei + eo);
        int64_t f = ring(fi + fo);
        _zi = ring(Comm::rank() * e * f + f * _bmt._a + e * _bmt._b + _bmt._c);
    }

    return this;
}

MulExecutor *MulExecutor::setBmt(BMT bmt) {
    _bmt = bmt;
    return this;
}