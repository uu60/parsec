//
// Created by 杜建璋 on 2024/11/15.
//

#include "compute/single/bool/BoolEqualOperator.h"

#include "compute/single/bool/BoolAndOperator.h"
#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"

BoolEqualOperator *BoolEqualOperator::execute() {
    if (Comm::isServer()) {
        int64_t diff = _xi ^ _yi;
        // z0 = ~z0
        if (Comm::rank() == 0) {
            diff = ~diff;
        }
        // if equal, z will be all 1.
        _zi = diff & 1;
        auto bmt = _bmt == nullptr ? IntermediateDataSupport::pollBitwiseBmts(1, _width)[0] : *_bmt;
        if (_bmt != nullptr) {
            bmt = *_bmt;
        } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
            bmt = IntermediateDataSupport::pollBitwiseBmts(1, _width)[0];
        }
        for (int i = 0; i < _width - 1; i++) {
            _zi = BoolAndOperator(_zi, (diff >> (i + 1)) & 1, 1, _taskTag, 0, NO_CLIENT_COMPUTE).setBmt(_bmt != nullptr || Conf::BMT_METHOD == Conf::BMT_BACKGROUND ? &bmt : nullptr)->execute()->_zi;
        }
    }
    return this;
}

BoolEqualOperator * BoolEqualOperator::setBmt(BitwiseBmt *bmt) {
    _bmt = bmt;
    return this;
}
