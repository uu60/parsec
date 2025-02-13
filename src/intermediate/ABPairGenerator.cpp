//
// Created by 杜建璋 on 2024/12/2.
//

#include "intermediate/ABPairGenerator.h"

#include "compute/single/bool/BoolToArithExecutor.h"
#include "utils/Math.h"


ABPairGenerator &ABPairGenerator::getInstance() {
    static ABPairGenerator instance;
    return instance;
}

ABPairGenerator *ABPairGenerator::execute() {
    _pair._b = Math::randInt();
    _pair._a = BoolToArithExecutor(_pair._b, 64, _taskTag, 0, NO_CLIENT_COMPUTE).execute()->_zi;
    return this;
}

ABPairGenerator *ABPairGenerator::reconstruct(int clientRank) {
    return this;
}
