//
// Created by 杜建璋 on 2025/1/8.
//

#include "api/BoolSecret.h"

#include "compute/single/bool/BoolAndExecutor.h"
#include "compute/single/bool/BoolExecutor.h"
#include "compute/single/bool/BoolLessExecutor.h"
#include "compute/single/bool/BoolMutexExecutor.h"
#include "compute/single/bool/BoolToArithExecutor.h"
#include "compute/single/bool/BoolXorExecutor.h"

BoolSecret::BoolSecret(int64_t x, int l, int16_t taskTag) : _data(x), _l(l), _taskTag(taskTag) {}

BoolSecret BoolSecret::task(int16_t taskTag) const {
    return {_data, _l, taskTag};
}

BoolSecret BoolSecret::share(int clientRank) const {
    return {BoolExecutor(_data, _l, _taskTag, 0, clientRank)._zi, _l, _taskTag};
}

BoolSecret BoolSecret::reconstruct(int clientRank) const {
    return {BoolExecutor(_data, _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).reconstruct(clientRank)->_result, _l, _taskTag};
}

int64_t BoolSecret::get() const {
    return _data;
}

BoolSecret BoolSecret::xor_(BoolSecret yi) const {
    return {BoolXorExecutor(_data, yi.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}

BoolSecret BoolSecret::and_(BoolSecret yi) const {
    return {BoolAndExecutor(_data, yi.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}

BoolSecret BoolSecret::arithmetic() const {
    return {BoolToArithExecutor(_data, _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}

BitSecret BoolSecret::lessThan(BoolSecret yi) const {
    return BitSecret(BoolLessExecutor(_data, yi.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

BoolSecret BoolSecret::mux(BoolSecret yi, BitSecret cond_i) const {
    return {BoolMutexExecutor(_data, yi.get(), cond_i.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}
