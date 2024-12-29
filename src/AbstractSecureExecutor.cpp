//
// Created by 杜建璋 on 2024/7/7.
//

#include "AbstractSecureExecutor.h"

#include <iostream>

#include "utils/Math.h"

/**
 * 0 is preserved for OtBmtGenerator.
 * 1 is preserved for ABPairGenerator.
 * 2 is preserved for BaseOtExecutor
 */
const int16_t AbstractSecureExecutor::_preservedObjTags = 3;
std::atomic_int16_t AbstractSecureExecutor::_currentObjTag = _preservedObjTags;

int AbstractSecureExecutor::buildTag(int16_t msgTag) const {
    return (_objTag << 16) | msgTag;
}

std::vector<int16_t> AbstractSecureExecutor::nextMsgTags(int num) {
    std::vector<int16_t> ret;
    ret.reserve(num);
    for (int i = 0; i < num; i++) {
        ret.push_back(_currentMsgTag++);
    }
    return ret;
}

std::vector<int16_t> AbstractSecureExecutor::nextObjTags(int num) {
    std::vector<int16_t> ret;
    ret.reserve(num);
    for (int i = 0; i < num; i++) {
        if ((_currentObjTag << 16) < 0) {
            _currentObjTag = 0;
        }
        if (_currentObjTag >= 0 && _currentObjTag < _preservedObjTags) {
            _currentObjTag = static_cast<int16_t>(Math::ring(_currentObjTag + 1, 16));
            continue;
        }
        ret.push_back(_currentObjTag);
        _currentObjTag = static_cast<int16_t>(Math::ring(_currentObjTag + 1, 16));
    }
    return ret;
}

int64_t AbstractSecureExecutor::ring(int64_t raw) const {
    return Math::ring(raw, _l);
}
