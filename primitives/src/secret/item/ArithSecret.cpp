//
// Created by 杜建璋 on 2024/9/12.
//

#include <utility>

#include "secret/item/ArithSecret.h"

#include "compute/single/arith/ArithOperator.h"
#include "compute/single/bool/BoolOperator.h"
#include "compute/single/arith/ArithAddOperator.h"
#include "compute/single/arith/ArithMultiplyOperator.h"
#include "compute/single/arith/ArithLessOperator.h"
#include "compute/single/arith/ArithMutexOperator.h"
#include "compute/single/arith/ArithToBoolOperator.h"
#include "utils/Log.h"

ArithSecret::ArithSecret() = default;

ArithSecret::ArithSecret(int64_t x, int l, int taskTag) : _data(x), _width(l), _taskTag(taskTag) {}

ArithSecret ArithSecret::task(int taskTag) const {
    return {_data, _width, taskTag};
}

ArithSecret ArithSecret::msg(int msgTagOffset) const {
    return {};
}

ArithSecret ArithSecret::share(int clientRank) const {
    return {ArithOperator(_data, _width, _taskTag, 0, clientRank)._zi, _width, _taskTag};
}

ArithSecret ArithSecret::reconstruct(int clientRank) const {
    return {ArithOperator(_data, _width, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).reconstruct(clientRank)->_result, _width, _taskTag};
}

ArithSecret ArithSecret::add(ArithSecret yi) const {
    return {ArithAddOperator(_data, yi._data, _width, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi, _width, _taskTag};
}

ArithSecret ArithSecret::mul(ArithSecret yi) const {
    return {ArithMultiplyOperator(_data, yi._data, _width, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi, _width, _taskTag};
}

ArithSecret ArithSecret::boolean() const {
    return {ArithToBoolOperator(_data, _width, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi, _width, _taskTag};
}

BitSecret ArithSecret::lessThan(ArithSecret yi) const {
    return BitSecret(ArithLessOperator(_data, yi._data, _width, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi, _taskTag);
}

BitSecret ArithSecret::getBit(int n) const {
    return BitSecret(Math::getBit(_data, n), _taskTag);
}

ArithSecret ArithSecret::mux(ArithSecret yi, BitSecret cond_i) const {
    return {ArithMutexOperator(_data, yi._data, cond_i.get(), _width, _taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi, _width, _taskTag};
}
