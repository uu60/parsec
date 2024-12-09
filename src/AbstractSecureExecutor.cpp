//
// Created by 杜建璋 on 2024/7/7.
//

#include "AbstractSecureExecutor.h"

#include <iostream>

#include "utils/Math.h"

/**
 * 0 is preserved for OtBmtGenerator.
 * 1 is preserved for ABPairGenerator.
 */
std::set<int32_t> AbstractSecureExecutor::_preservedObjTags = {0, 1};
int32_t AbstractSecureExecutor::_currentObjTag = 2;

int AbstractSecureExecutor::buildTag(int8_t msgTag) const {
    return (_objTag << 8) + msgTag;
}

std::vector<int8_t> AbstractSecureExecutor::nextMsgTags(int num) {
    std::vector<int8_t> ret;
    ret.reserve(num);
    for (int i = 0; i < num; i++) {
        ret.push_back(_currentMsgTag++);
    }
    return ret;
}

std::vector<int32_t> AbstractSecureExecutor::nextObjTags(int num) {
    std::vector<int32_t> ret;
    ret.reserve(num);
    for (int i = 0; i < num; i++) {
        if (_preservedObjTags.count(_currentObjTag) > 0) {
            _currentObjTag = static_cast<int32_t>(Math::ring(_currentObjTag + 1, 24));
            continue;
        }
        ret.push_back(_currentObjTag);
        _currentObjTag = static_cast<int32_t>(Math::ring(_currentObjTag + 1, 24));
    }
    return ret;
}

int64_t AbstractSecureExecutor::ring(int64_t raw) const {
    return Math::ring(raw, _l);
}
