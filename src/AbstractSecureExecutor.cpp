//
// Created by 杜建璋 on 2024/7/7.
//

#include "AbstractSecureExecutor.h"

#include <iostream>

#include "utils/Math.h"

int AbstractSecureExecutor::buildTag(int16_t msgTag) const {
    return (_taskTag << 16) | msgTag;
}

int64_t AbstractSecureExecutor::ring(int64_t raw) const {
    return Math::ring(raw, _l);
}
