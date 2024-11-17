//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/bool/BitwiseXorExecutor.h"

#include "compute/arith/AddExecutor.h"

BitwiseXorExecutor *BitwiseXorExecutor::execute() {
    for (int i = 0; i < _l; i++) {
        _zi += (AddExecutor((_xi >> i) & 1, (_yi >> i) & 1, 1, true).execute()->_zi) << i;
    }
    return this;
}
