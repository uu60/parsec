//
// Created by 杜建璋 on 2024/9/12.
//

#include <utility>

#include "api/ArithSecret.h"

#include "comm/Comm.h"
#include "compute/arith/ArithExecutor.h"
#include "compute/bool/BoolExecutor.h"
#include "compute/arith/ArithAddExecutor.h"
#include "compute/arith/ArithMultiplyExecutor.h"
#include "compute/arith/ArithLessExecutor.h"
#include "compute/arith/ArithMutexExecutor.h"
#include "compute/arith/ArithToBoolExecutor.h"
#include "compute/bool/BoolAndExecutor.h"
#include "compute/bool/BoolLessExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"
#include "compute/bool/BoolXorExecutor.h"
#include "utils/Log.h"

ArithSecret::ArithSecret(int64_t x, int l, int16_t taskTag) : _data(x), _l(l), _taskTag(taskTag) {}

ArithSecret ArithSecret::task(int16_t taskTag) const {
    return {_data, _l, taskTag};
}

ArithSecret ArithSecret::share(int clientRank) const {
    return {ArithExecutor(_data, _l, _taskTag, 0, clientRank)._zi, _l, _taskTag};
}

ArithSecret ArithSecret::reconstruct(int clientRank) const {
    return {ArithExecutor(_data, _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).reconstruct(clientRank)->_result, _l, _taskTag};
}

int64_t ArithSecret::get() const {
    return _data;
}

ArithSecret ArithSecret::add(ArithSecret yi) const {
    return {ArithAddExecutor(_data, yi.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}

ArithSecret ArithSecret::mul(ArithSecret yi) const {
    return {ArithMultiplyExecutor(_data, yi.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}

ArithSecret ArithSecret::boolean() const {
    return {ArithToBoolExecutor(_data, _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}

BitSecret ArithSecret::lessThan(ArithSecret yi) const {
    return BitSecret(ArithLessExecutor(_data, yi.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

ArithSecret ArithSecret::mux(ArithSecret yi, BitSecret cond_i) const {
    return {ArithMutexExecutor(_data, yi.get(), cond_i.get(), _l, _taskTag, 0, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi, _l, _taskTag};
}
