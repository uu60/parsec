//
// Created by 杜建璋 on 2024/9/12.
//

#include <utility>

#include "api/IntSecret.h"
#include "compute/ArithExecutor.h"
#include "compute/BoolExecutor.h"
#include "compute/arith/AddExecutor.h"
#include "compute/arith/MulExecutor.h"
#include "compute/arith/LessThanArithExecutor.h"
#include "compute/arith/MuxArithExecutor.h"

IntSecret::IntSecret(int64_t x, int l) {
    _data = x;
    _l = l;
}

IntSecret IntSecret::arithShare() const {
    return {ArithExecutor(_data, _l, false)._zi, _l};
}

IntSecret IntSecret::boolShare() const {
    return {BoolExecutor(_data, _l, false)._zi, _l};
}

[[nodiscard]] IntSecret IntSecret::arithReconstruct() const {
    return {ArithExecutor(_data, _l, true).reconstruct()->_result, _l};
}

[[nodiscard]] IntSecret IntSecret::boolReconstruct() const {
    return {BoolExecutor(_data, _l, true).reconstruct()->_result, _l};
}

int64_t IntSecret::get() const {
    return _data;
}

IntSecret IntSecret::add(IntSecret yi) const {
    return {AddExecutor(_data, yi.get(), _l, true).execute()->_zi, _l};
}

IntSecret IntSecret::mul(IntSecret yi, BMT bmt) const {
    return {MulExecutor(_data, yi.get(), _l, true).setBmt(bmt)->execute()->_zi, _l};
}

IntSecret IntSecret::boolean(std::vector<BMT> bmts) const {
    return {ArithExecutor(_data, _l, true).boolZi(std::move(bmts)), _l};
}

IntSecret IntSecret::arithmetic() const {
    return {BoolExecutor(_data, _l, true).arithZi(), _l};
}

BitSecret IntSecret::arithLessThan(IntSecret yi) const {
    return BitSecret(LessThanArithExecutor(_data, yi.get(), _l, true).execute()->_sign);
}

IntSecret IntSecret::mux(IntSecret yi, BitSecret cond_i, BMT bmt0, BMT bmt1) const {
    return {MuxArithExecutor(_data, yi.get(), _l, cond_i.get(), true).setBmts(bmt0, bmt1)->execute()->_zi, _l};
}
