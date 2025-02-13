//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/single/bool/BoolAndExecutor.h"

#include "compute/single/arith/ArithMultiplyExecutor.h"
#include "comm/Comm.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolAndExecutor *BoolAndExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        int64_t ei = 0, fi = 0;
        BitwiseBmt bmt;
        if (_bmt != nullptr) {
            bmt = *_bmt;
        } else if (Conf::BMT_BACKGROUND) {
            bmt = IntermediateDataSupport::pollBitwiseBmts(1, 64)[0];
        } else {
            bmt = BitwiseBmtGenerator(_width, _taskTag, _currentMsgTag).execute()->_bmt;
        }

        for (int i = 0; i < _width; i++) {
            bool x_bit_i = Math::getBit(_xi, i);
            bool y_bit_i = Math::getBit(_yi, i);
            bool a_bit_i = Math::getBit(bmt._a, i);
            bool b_bit_i = Math::getBit(bmt._b, i);
            ei = Math::changeBit(ei, i, x_bit_i ^ a_bit_i);
            fi = Math::changeBit(fi, i, y_bit_i ^ b_bit_i);
        }

        std::vector efi = {ei, fi};
        std::vector<int64_t> efo;
        Comm::serverSend(efi, _width, buildTag(_currentMsgTag));
        Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
        int64_t e = ring(ei ^ efo[0]);
        int64_t f = ring(fi ^ efo[1]);
        for (int i = 0; i < _width; i++) {
            bool e_bit_i = Math::getBit(e, i);
            bool f_bit_i = Math::getBit(f, i);
            bool a_bit_i = Math::getBit(bmt._a, i);
            bool b_bit_i = Math::getBit(bmt._b, i);
            bool c_bit_i = Math::getBit(bmt._c, i);
            _zi = Math::changeBit(
                _zi, i,
                (Comm::rank() & e_bit_i & f_bit_i) ^ (f_bit_i & a_bit_i) ^ (e_bit_i & b_bit_i) ^ c_bit_i);
        }
    }
    return this;
}

int16_t BoolAndExecutor::msgTagCount(int width) {
    return BitwiseBmtGenerator::msgTagCount(width);
}

BoolAndExecutor *BoolAndExecutor::setBmt(BitwiseBmt *bmt) {
    _bmt = bmt;
    return this;
}
