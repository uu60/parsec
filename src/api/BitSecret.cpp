//
// Created by 杜建璋 on 2024/9/13.
//

#include "api/BitSecret.h"

#include "compute/ArithExecutor.h"
#include "compute/arith/MuxArithExecutor.h"
#include "compute/bool/BitwiseAndExecutor.h"
#include "compute/bool/BitwiseXorExecutor.h"
#include "utils/Comm.h"

BitSecret::BitSecret(bool x) {
    _data = x;
}

BitSecret BitSecret::share() const {
    return BitSecret(ArithExecutor(_data, 1, false)._zi);
}

BitSecret BitSecret::not_() const {
    return BitSecret(_data ^ Comm::rank());
}

BitSecret BitSecret::xor_(BitSecret yi) const {
    return BitSecret(BitwiseXorExecutor(_data, yi.get(), 1, true).execute()->_zi);
}

BitSecret BitSecret::and_(BitSecret yi, BMT bmt) const {
    return BitSecret(BitwiseAndExecutor(_data, yi.get(), 1, true).setBmts({bmt})->execute()->_zi);
}

BitSecret BitSecret::or_(BitSecret yi, BMT bmt) const {
    return xor_(yi).xor_(and_(yi, bmt));
}

BitSecret BitSecret::mux(BitSecret yi, BitSecret cond_i, BMT bmt0, BMT bmt1) const {
    return BitSecret(MuxArithExecutor(_data, yi.get(), 1, cond_i.get(), true).setBmts(bmt0, bmt1)->execute()->_zi);
}

BitSecret BitSecret::reconstruct() const {
    return BitSecret(ArithExecutor(_data, 1, true).reconstruct()->_result);
}

bool BitSecret::get() const {
    return _data;
}

















