//
// Created by 杜建璋 on 2024/9/13.
//

#include "api/BitSecret.h"

#include "compute/ArithExecutor.h"
#include "compute/arith/ArithMuxExecutor.h"
#include "compute/bool/BoolAndExecutor.h"
#include "compute/bool/BoolXorExecutor.h"
#include "comm/IComm.h"

BitSecret::BitSecret(bool x, int32_t objTag) : _data(x), _objTag(objTag) {}

BitSecret BitSecret::share() const {
    return BitSecret(BoolExecutor(_data, 1, _objTag, 0, IComm::impl->rank())._zi, _objTag);
}

BitSecret BitSecret::not_() const {
    return BitSecret(_data ^ IComm::impl->rank(), _objTag);
}

BitSecret BitSecret::xor_(BitSecret yi) const {
    return BitSecret(BoolXorExecutor(_data, yi.get(), 1, _objTag, 0, -1).execute()->_zi, _objTag);
}

BitSecret BitSecret::and_(BitSecret yi) const {
    return BitSecret(BoolAndExecutor(_data, yi.get(), 1, _objTag, 0, -1).execute()->_zi, _objTag);
}

BitSecret BitSecret::or_(BitSecret yi) const {
    return xor_(yi).xor_(and_(yi));
}

BitSecret BitSecret::mux(BitSecret yi, BitSecret cond_i) const {
    return BitSecret(ArithMuxExecutor(_data, yi.get(), 1, cond_i.get(), _objTag, 0, -1).execute()->_zi, _objTag);
}

BitSecret BitSecret::reconstruct(int clientRank) const {
    return BitSecret(ArithExecutor(_data, 1, _objTag, 0, -1).reconstruct(clientRank)->_result, _objTag);
}

bool BitSecret::get() const {
    return _data;
}

















