//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/arith/MuxArithExecutor.h"

#include "compute/arith/MulExecutor.h"
#include "utils/Comm.h"

MuxArithExecutor::MuxArithExecutor(int64_t x, int64_t y, int l, bool cond_i, bool local) : ArithExecutor(x, y, l, local) {
    _cond_i = local ? cond_i : ArithExecutor(cond_i, 1, false)._zi;
}

MuxArithExecutor *MuxArithExecutor::execute() {
    if (Comm::isServer()) {
        int64_t a_cond_i = _l == 1 ? _cond_i : BoolExecutor(_cond_i, _l, true).arithZi();
        int64_t cx = MulExecutor(a_cond_i, _xi, _l, true).setBmt(_bmt0)->execute()->_zi;
        int64_t cy = MulExecutor(a_cond_i, _yi, _l, true).setBmt(_bmt1)->execute()->_zi;
        _zi = ring(cx + _yi - cy);
    }
    return this;
}

string MuxArithExecutor::tag() const {
    return "[MuxArithExecutor]";
}

MuxArithExecutor * MuxArithExecutor::setBmts(BMT bmt0, BMT bmt1) {
    _bmt0 = bmt0;
    _bmt1 = bmt1;
    return this;
}
