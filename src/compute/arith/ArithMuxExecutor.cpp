//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/arith/ArithMuxExecutor.h"

#include <folly/futures/Future.h>

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
        BoolToArithExecutor e(_cond_i, _l, _objTag, _currentMsgTag++, -1);
        int64_t a_cond_i = _l == 1 ? _cond_i : e.execute()->_zi;

        auto offset0 = static_cast<int8_t>(_currentMsgTag + ArithMulExecutor::msgNum());
        _currentMsgTag = offset0;
        auto offset1 = static_cast<int8_t>(_currentMsgTag + ArithMulExecutor::msgNum());
        _currentMsgTag = offset1;

        int64_t cx, cy;
        auto f0 = folly::via(&System::_threadPool, [a_cond_i, this, offset0] {
            auto mul0 = ArithMulExecutor(a_cond_i, _xi, _l, _objTag, offset0, -1);
            return mul0.execute()->_zi;
        });
        auto f1 = folly::via(&System::_threadPool, [a_cond_i, this, offset1] {
            auto mul1 = ArithMulExecutor(a_cond_i, _yi, _l, _objTag, offset1, -1);
            return mul1.execute()->_zi;
        });

        cx = f0.wait().value();
        cy = f1.wait().value();

        _zi = ring(cx + _yi - cy);
    }
    return this;
}

std::string ArithMuxExecutor::className() const {
    return "[MuxArithExecutor]";
}
