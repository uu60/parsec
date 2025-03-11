//
// Created by 杜建璋 on 2025/1/8.
//

#include "secret/item/BoolSecret.h"

#include "compute/single/bool/BoolAndExecutor.h"
#include "compute/single/bool/BoolExecutor.h"
#include "compute/single/bool/BoolLessExecutor.h"
#include "compute/single/bool/BoolMutexExecutor.h"
#include "compute/single/bool/BoolToArithExecutor.h"
#include "compute/single/bool/BoolXorExecutor.h"

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
    return {BoolExecutor(_data, _width, _taskTag, _currentMsgTag, clientRank)._zi, _width, _taskTag, _currentMsgTag};
}

BoolSecret BoolSecret::reconstruct(int clientRank) const {
    return {
        BoolExecutor(_data, _width, _taskTag, _currentMsgTag, AbstractSecureExecutor::NO_CLIENT_COMPUTE).reconstruct(clientRank)->
        _result,
        _width, _taskTag, _currentMsgTag
    };
}

BoolSecret BoolSecret::xor_(BoolSecret yi) const {
    return {
        BoolXorExecutor(_data, yi._data, _width, _taskTag, _currentMsgTag, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}

BoolSecret BoolSecret::and_(BoolSecret yi) const {
    return {
        BoolAndExecutor(_data, yi._data, _width, _taskTag, _currentMsgTag, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}

BoolSecret BoolSecret::arithmetic() const {
    return {
        BoolToArithExecutor(_data, _width, _taskTag, _currentMsgTag, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}

BitSecret BoolSecret::lessThan(BoolSecret yi) const {
    return BitSecret(
        BoolLessExecutor(_data, yi._data, _width, _taskTag, _currentMsgTag, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->
        _zi, _taskTag);
}

BitSecret BoolSecret::getBit(int n) const {
    return BitSecret(Math::getBit(_data, n), _taskTag);
}

BoolSecret BoolSecret::mux(BoolSecret yi, BitSecret cond_i) const {
    return {
        BoolMutexExecutor(_data, yi._data, cond_i._data, _width, _taskTag, _currentMsgTag,
                          AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zi,
        _width, _taskTag, _currentMsgTag
    };
}
