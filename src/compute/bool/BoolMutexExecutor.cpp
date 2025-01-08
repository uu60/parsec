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
        _cond_i = ring(-1ll);
    }
}

BoolMutexExecutor * BoolMutexExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        std::vector<Bmt> bmts0, bmts1;

        if (_bmts != nullptr) {
            bmts0.reserve(BoolAndExecutor::needsBmts(_l));
            bmts1.reserve(BoolAndExecutor::needsBmts(_l));
            for (int i = 0; i < BoolAndExecutor::needsBmts(_l); i++) {
                bmts0.emplace_back(_bmts->at(i));
                bmts1.emplace_back(_bmts->at(BoolAndExecutor::needsBmts(_l) + i));
            }
        } else {
            bmts0 = IntermediateDataSupport::pollBmts(BoolAndExecutor::needsBmts(_l));
            bmts1 = IntermediateDataSupport::pollBmts(BoolAndExecutor::needsBmts(_l));
        }

        int64_t cx, cy;
        auto f0 = System::_threadPool.push([this, &bmts0](int _) {
            auto executor = BoolAndExecutor(_cond_i, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            return executor.setBmts(&bmts0)->execute()->_zi;
        });
        auto f1 = System::_threadPool.push([this, &bmts1](int _) {
            auto mul1 = BoolAndExecutor(_cond_i, _yi, _l, _taskTag,
                                         static_cast<int16_t>(_currentMsgTag + BoolAndExecutor::needsMsgTags()), NO_CLIENT_COMPUTE);
            return mul1.setBmts(&bmts1)->execute()->_zi;
        });
        cx = f0.get();
        cy = f1.get();
        _zi = ring(cx ^ _yi ^ cy);
    }
    return this;
}

int BoolMutexExecutor::needsBmts(int l) {
    return 2 * BoolAndExecutor::needsBmts(l);
}

BoolMutexExecutor * BoolMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

int16_t BoolMutexExecutor::needsMsgTags() {
    return static_cast<int16_t>(2 * BoolAndExecutor::needsMsgTags());
}
