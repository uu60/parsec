//
// Created by 杜建璋 on 2024/10/23.
//

#include "compute/arith/ArithMutexExecutor.h"

#include "compute/arith/ArithMultiplyExecutor.h"
#include "comm/Comm.h"
#include "compute/bool/BoolExecutor.h"
#include "compute/bool/BoolToArithExecutor.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithMutexExecutor::ArithMutexExecutor(int64_t x, int64_t y, bool cond, int l, int16_t taskTag, int16_t msgTagOffset,
                                       int clientRank) : ArithExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    if (clientRank < 0 && _l > 1) {
        _cond_i = BoolToArithExecutor(cond, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zi;
    } else {
        _cond_i = ArithExecutor(cond, _l, _taskTag, _currentMsgTag, clientRank)._zi;
    }
}

ArithMutexExecutor *ArithMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        Bmt bmt0;
        if (_bmts != nullptr) {
            bmt0 = _bmts->at(0);
        } else if (Conf::INTERM_PREGENERATED) {
            bmt0 = IntermediateDataSupport::pollBmts(1, _l)[0];
        }
        int64_t cx, cy;
        auto f = System::_threadPool.push([&](int) {
            auto mul0 = ArithMultiplyExecutor(_cond_i, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            return mul0.setBmt((_bmts == nullptr && !Conf::INTERM_PREGENERATED) ? nullptr : &bmt0)->execute()->_zi;
        });

        Bmt bmt1;
        if (_bmts != nullptr) {
            bmt1 = _bmts->at(1);
        } else if (Conf::INTERM_PREGENERATED) {
            bmt1 = IntermediateDataSupport::pollBmts(1, _l)[0];
        }
        auto mul1 = ArithMultiplyExecutor(_cond_i, _yi, _l, _taskTag,
                                          static_cast<int16_t>(_currentMsgTag + ArithMultiplyExecutor::needMsgTags(_l)),
                                          NO_CLIENT_COMPUTE);
        cy = mul1.setBmt((_bmts == nullptr && !Conf::INTERM_PREGENERATED) ? nullptr : &bmt1)->execute()->_zi;
        cx = f.get();
        _zi = ring(cx + _yi - cy);
    }
    return this;
}

std::string ArithMutexExecutor::className() const {
    return "[MuxArithExecutor]";
}

int16_t ArithMutexExecutor::needMsgTags(int l) {
    return std::max(static_cast<int16_t>(2 * ArithMultiplyExecutor::needMsgTags(l)),
                    BoolToArithExecutor::needMsgTags(l));
}

ArithMutexExecutor *ArithMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

std::pair<int, int> ArithMutexExecutor::needBmtsAndBits(int l) {
    return {2, l};
}
