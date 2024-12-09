//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/arith/ArithMuxExecutor.h"

#include "compute/arith/ArithMulExecutor.h"
#include "comm/IComm.h"
#include "compute/BoolExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"

ArithMuxExecutor::ArithMuxExecutor(int64_t x, int64_t y, bool c, int l, int32_t objTag, int8_t msgTagOffset,
                                   int clientRank) : ArithExecutor(x, y, l, objTag, msgTagOffset, clientRank) {
    _cond_i = ArithExecutor(c, 1, _objTag, _currentMsgTag++, clientRank)._zi;
}

ArithMuxExecutor *ArithMuxExecutor::execute() {
    if (IComm::impl->isServer()) {
        int64_t a_cond_i = _l == 1
                               ? _cond_i
                               : BoolToArithExecutor(_cond_i, _l, _objTag, _currentMsgTag++, -1).execute()->_zi;

        auto offset0 = static_cast<int8_t>(_currentMsgTag + ArithMulExecutor::msgNum());
        _currentMsgTag = offset0;
        auto offset1 = static_cast<int8_t>(_currentMsgTag + ArithMulExecutor::msgNum());
        _currentMsgTag = offset1;

        int64_t cx, cy;
        auto f0 = System::_threadPool.push([a_cond_i, this, offset0](int _) {
            auto mul0 = ArithMulExecutor(a_cond_i, _xi, _l, _objTag, offset0, -1);
            return mul0.execute()->_zi;
        });
        auto f1 = System::_threadPool.push([a_cond_i, this, offset1](int _) {
            auto mul1 = ArithMulExecutor(a_cond_i, _yi, _l, _objTag, offset1, -1);
            return mul1.execute()->_zi;
        });

        cx = f0.get();
        cy = f1.get();

        _zi = ring(cx + _yi - cy);
    }
    return this;
}

std::string ArithMuxExecutor::className() const {
    return "[MuxArithExecutor]";
}
