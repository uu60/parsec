//
// Created by 杜建璋 on 2025/1/8.
//

#include "compute/bool/BoolMutexExecutor.h"

#include "compute/bool/BoolAndExecutor.h"
#include "intermediate/IntermediateDataSupport.h"

BoolMutexExecutor::BoolMutexExecutor(int64_t x, int64_t y, bool cond, int l, int16_t taskTag, int16_t msgTagOffset,
                                     int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    _cond_i = BoolExecutor(cond, 1, _taskTag, _currentMsgTag, clientRank)._zi;
    if (_cond_i) {
        // Set to all 1 on each bit
        _cond_i = ring(-1ll);
    }
}

BoolMutexExecutor *BoolMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        std::vector<Bmt> temp0;
        auto [nb, nl] = BoolAndExecutor::needBmtsWithBits(_l);

        if (_bmts != nullptr) {
            temp0.reserve(nb);
            for (int i = 0; i < nb; i++) {
                temp0.push_back(_bmts->at(i));
            }
        } else if (Conf::INTERM_PREGENERATED) {
            temp0 = IntermediateDataSupport::pollBmts(nb, nl);
        }

        int64_t cx, cy;
        auto f = System::_threadPool.push([&](int) {
            BoolAndExecutor e(_cond_i, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            if (_bmts != nullptr || Conf::INTERM_PREGENERATED) {
                e.setBmts(&temp0);
            }
            return e.execute()->_zi;
        });

        std::vector<Bmt> temp1;
        if (_bmts != nullptr) {
            temp1.reserve(nb);
            for (int i = nb; i < nb * 2; i++) {
                temp1.push_back(_bmts->at(i));
            }
        }
        cy = BoolAndExecutor(_cond_i, _yi, _l, _taskTag,
                                   static_cast<int16_t>(_currentMsgTag + BoolAndExecutor::needMsgTags(_l)),
                                   NO_CLIENT_COMPUTE).setBmts(_bmts == nullptr ? nullptr : &temp1)->execute()->_zi;
        cx = f.get();
        _zi = ring(cx ^ _yi ^ cy);
    }
    return this;
}

BoolMutexExecutor *BoolMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

int16_t BoolMutexExecutor::needMsgTags(int l) {
    return static_cast<int16_t>(2 * BoolAndExecutor::needMsgTags(l));
}

std::pair<int, int> BoolMutexExecutor::needBmtsWithBits(int l) {
    auto needed = BoolAndExecutor::needBmtsWithBits(l);
    return {2 * needed.first, needed.second};
}
