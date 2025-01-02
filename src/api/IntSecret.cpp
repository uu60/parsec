//
// Created by 杜建璋 on 2024/9/12.
//

#include <utility>

#include "api/IntSecret.h"

#include "comm/IComm.h"
#include "compute/arith/ArithExecutor.h"
#include "compute/bool/BoolExecutor.h"
#include "compute/arith/ArithAddExecutor.h"
#include "compute/arith/ArithMultiplyExecutor.h"
#include "compute/arith/ArithLessThanExecutor.h"
#include "compute/arith/ArithMutexExecutor.h"
#include "compute/arith/ArithToBoolExecutor.h"
#include "compute/bool/BoolAndExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"
#include "compute/bool/BoolXorExecutor.h"
#include "utils/Log.h"

IntSecret::IntSecret(int64_t x, int l, int16_t taskTag) : _data(x), _l(l), _taskTag(taskTag) {}

IntSecret IntSecret::task(int16_t taskTag) const {
    return {_data, _l, taskTag};
}

IntSecret IntSecret::arithShare(int clientRank) const {
    return {ArithExecutor(_data, _l, _taskTag, 0, clientRank)._zi, _l, _taskTag};
}

IntSecret IntSecret::boolShare(int clientRank) const {
    return {BoolExecutor(_data, _l, _taskTag, 0, clientRank)._zi, _l, _taskTag};
}

IntSecret IntSecret::arithReconstruct(int clientRank) const {
    return {ArithExecutor(_data, _l, _taskTag, 0, -1).reconstruct(clientRank)->_result, _l, _taskTag};
}

IntSecret IntSecret::boolReconstruct(int clientRank) const {
    return {BoolExecutor(_data, _l, _taskTag, 0, -1).reconstruct(clientRank)->_result, _l, _taskTag};
}

int64_t IntSecret::get() const {
    return _data;
}

IntSecret IntSecret::add(IntSecret yi) const {
    return {ArithAddExecutor(_data, yi.get(), _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}

IntSecret IntSecret::mul(IntSecret yi) const {
    return {ArithMultiplyExecutor(_data, yi.get(), _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}

IntSecret IntSecret::xor_(IntSecret yi) const {
    return {BoolXorExecutor(_data, yi.get(), _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}

IntSecret IntSecret::and_(IntSecret yi) const {
    return {BoolAndExecutor(_data, yi.get(), _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}

IntSecret IntSecret::boolean() const {
    return {ArithToBoolExecutor(_data, _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}

IntSecret IntSecret::arithmetic() const {
    return {BoolToArithExecutor(_data, _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}

BitSecret IntSecret::arithLessThan(IntSecret yi) const {
    return BitSecret(ArithLessThanExecutor(_data, yi.get(), _l, _taskTag, 0, -1).execute()->_sign, _taskTag);
}

IntSecret IntSecret::mux(IntSecret yi, BitSecret cond_i) const {
    return {ArithMutexExecutor(_data, yi.get(), cond_i.get(), _l, _taskTag, 0, -1).execute()->_zi, _l, _taskTag};
}
