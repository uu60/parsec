//
// Created by 杜建璋 on 2024/9/13.
//

#include "api/BitSecret.h"

#include "compute/arith/ArithExecutor.h"
#include "compute/arith/ArithMutexExecutor.h"
#include "compute/bool/BoolAndExecutor.h"
#include "compute/bool/BoolXorExecutor.h"
#include "comm/IComm.h"

BitSecret::BitSecret(bool x, int16_t taskTag) : _data(x), _taskTag(taskTag) {}

BitSecret BitSecret::task(int16_t taskTag) {
    return BitSecret(_data, taskTag);
}

BitSecret BitSecret::share(int clientRank) const {
    return BitSecret(BoolExecutor(_data, 1, _taskTag, 0, clientRank)._zi, _taskTag);
}

BitSecret BitSecret::not_() const {
    return BitSecret(_data ^ IComm::impl->rank(), _taskTag);
}

BitSecret BitSecret::xor_(BitSecret yi) const {
    return BitSecret(BoolXorExecutor(_data, yi.get(), 1, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

BitSecret BitSecret::and_(BitSecret yi) const {
    return BitSecret(BoolAndExecutor(_data, yi.get(), 1, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

BitSecret BitSecret::or_(BitSecret yi) const {
    return xor_(yi).xor_(and_(yi));
}

BitSecret BitSecret::mux(BitSecret yi, BitSecret cond_i) const {
    return BitSecret(ArithMutexExecutor(_data, yi.get(), cond_i.get(), 1, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

BitSecret BitSecret::reconstruct(int clientRank) const {
    return BitSecret(ArithExecutor(_data, 1, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).reconstruct(clientRank)->_result, _taskTag);
}

bool BitSecret::get() const {
    return _data;
}
