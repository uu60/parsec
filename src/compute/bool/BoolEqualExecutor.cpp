//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/bool/BoolEqualExecutor.h"

#include "compute/bool/BoolAndExecutor.h"
#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"

BoolEqualExecutor *BoolEqualExecutor::execute() {
    if (Comm::isServer()) {
        int64_t diff = _xi ^ _yi;
        // z0 = ~z0
        if (Comm::rank() == 0) {
            diff = ~diff;
        }
        // if equal, z will be all 1.
        _zi = diff & 1;
        for (int i = 0; i < _l - 1; i++) {
            std::vector<Bmt> bmts = {_bmts == nullptr ? IntermediateDataSupport::pollBmts(1, 1)[0] : (*_bmts)[i]};
            _zi = BoolAndExecutor(_zi, (diff >> (i + 1)) & 1, 1, _taskTag, 0, NO_CLIENT_COMPUTE).setBmts(&bmts)->execute()->_zi;
        }
    }
    return this;
}

std::string BoolEqualExecutor::className() const {
    return "EqualBoolExecutor";
}

BoolEqualExecutor *BoolEqualExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

std::pair<int, int> BoolEqualExecutor::needBmtsAndBits(int l) {
    return {l - 1, 1};
}
