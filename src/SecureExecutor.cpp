//
// Created by 杜建璋 on 2024/7/7.
//

#include "SecureExecutor.h"

#include "utils/Math.h"

SecureExecutor::SecureExecutor(int l) {
    _l = l;
}

SecureExecutor *SecureExecutor::reconstruct() {
    throw std::runtime_error("This method cannot be called!");
}

int64_t SecureExecutor::ring(int64_t raw) const {
    return Math::ring(raw, _l);
}

SecureExecutor *SecureExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string SecureExecutor::tag() const {
    throw std::runtime_error("This method cannot be called!");
}