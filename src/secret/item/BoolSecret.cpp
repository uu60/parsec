//
// Created by 杜建璋 on 2025/1/8.
//

#include "secret/item/BoolSecret.h"

#include "compute/single/bool/BoolAndOperator.h"
#include "compute/single/bool/BoolOperator.h"
#include "compute/single/bool/BoolLessOperator.h"
#include "compute/single/bool/BoolMutexOperator.h"
#include "compute/single/bool/BoolToArithOperator.h"
#include "compute/single/bool/BoolXorOperator.h"

BoolSecret::BoolSecret() = default;

BoolSecret::BoolSecret(int64_t x, int l, int taskTag, int msgTagOffset) : _data(x), _width(l),
    _taskTag(taskTag), _currentMsgTag(msgTagOffset) {
}

BoolSecret BoolSecret::task(int taskTag) const {
    return {_data, _width, taskTag, _currentMsgTag};
}

BoolSecret BoolSecret::msg(int msgTagOffset) const {
    return {_data, _width, _taskTag, msgTagOffset};
}

BoolSecret BoolSecret::share(int clientRank) const {
    return {BoolOperator(_data, _width, _taskTag, _currentMsgTag, clientRank)._zi, _width, _taskTag, _currentMsgTag};
}

BoolSecret BoolSecret::reconstruct(int clientRank) const {
    return {
        BoolOperator(_data, _width, _taskTag, _currentMsgTag, SecureOperator::NO_CLIENT_COMPUTE).reconstruct(clientRank)->
        _result,
        _width, _taskTag, _currentMsgTag
    };
}

BoolSecret BoolSecret::xor_(BoolSecret yi) const {
    return {
        BoolXorOperator(_data, yi._data, _width, _taskTag, _currentMsgTag, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}

BoolSecret BoolSecret::and_(BoolSecret yi) const {
    return {
        BoolAndOperator(_data, yi._data, _width, _taskTag, _currentMsgTag, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}

BoolSecret BoolSecret::arithmetic() const {
    return {
        BoolToArithOperator(_data, _width, _taskTag, _currentMsgTag, SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}

BitSecret BoolSecret::lessThan(BoolSecret yi) const {
    return BitSecret(
        BoolLessOperator(_data, yi._data, _width, _taskTag, _currentMsgTag, SecureOperator::NO_CLIENT_COMPUTE).execute()->
        _zi, _taskTag);
}

BitSecret BoolSecret::getBit(int n) const {
    return BitSecret(Math::getBit(_data, n), _taskTag);
}

BoolSecret BoolSecret::mux(BoolSecret yi, BitSecret cond_i) const {
    return {
        BoolMutexOperator(_data, yi._data, cond_i._data, _width, _taskTag, _currentMsgTag,
                          SecureOperator::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}
