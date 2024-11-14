//
// Created by 杜建璋 on 2024/9/13.
//

#include "api/BitSecret.h"
#include "bit/BitExecutor.h"
#include "bit/and/BitAndExecutor.h"
#include "bit/xor/BitXorExecutor.h"
#include "int/comparison/MuxExecutor.h"

BitSecret::BitSecret(bool x) {
    _data = x;
}

BitSecret BitSecret::share() const {
    return BitSecret(BitExecutor(_data, true)._zi);
}

BitSecret BitSecret::not_() const {
    return BitSecret(_data ^ Comm::rank());
}

BitSecret BitSecret::xor_(BitSecret yi) const {
    return BitSecret(BitXorExecutor(_data, yi.get(), false).execute(false)->_zi);
}

BitSecret BitSecret::and_(BitSecret yi, bool ai, bool bi, bool ci) const {
    return BitSecret(BitAndExecutor(_data, yi.get()).obtainBmt(ai, bi, ci)->execute(false)->_zi);
}

BitSecret BitSecret::or_(BitSecret yi, bool ai, bool bi, bool ci) const {
    return xor_(yi).xor_(and_(yi, ai, bi, ci));
}

BitSecret BitSecret::mux(BitSecret yi, BitSecret cond_i, bool ai, bool bi, bool ci) const {
    return BitSecret(MuxExecutor(_data, yi.get(), cond_i.get(), false).obtainBmt(ai, bi, ci)->execute(false)->_zi);
}

BitSecret BitSecret::reconstruct() const {
    return BitSecret(BitExecutor(_data, false).reconstruct()->_result);
}

bool BitSecret::get() const {
    return _data;
}

















