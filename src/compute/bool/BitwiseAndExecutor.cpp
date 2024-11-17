//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/bool/BitwiseAndExecutor.h"

#include "compute/arith/MulExecutor.h"
#include "utils/Comm.h"

BitwiseAndExecutor *BitwiseAndExecutor::execute() {
    if (Comm::isServer()) {
        for (int i = 0; i < _l; i++) {
            _zi += (MulExecutor((_xi >> i) & 1, (_yi >> i) & 1, 1, true).setBmt(_bmts[i])->execute()->_zi) << i;
        }
    }
    return this;
}

BitwiseAndExecutor * BitwiseAndExecutor::setBmts(std::vector<BMT> bmts) {
    _bmts = bmts;
    return this;
}
