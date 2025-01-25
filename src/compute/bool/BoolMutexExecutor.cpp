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
        // std::vector<Bmt> bmts0, bmts1;
        //
        // auto bmt = _bmt == nullptr ? IntermediateDataSupport::pollBmts(1)[0] : *_bmt;
        //
        // int64_t cx, cy;
        // auto f0 = System::_threadPool.push([&](int) {
        //     return BoolAndExecutor(_cond_i, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(&bmt)->
        //             execute()->_zi;
        // });
        // auto f1 = System::_threadPool.push([&](int) {
        //     return BoolAndExecutor(_cond_i, _yi, _l, _taskTag,
        //                            static_cast<int16_t>(_currentMsgTag + BoolAndExecutor::needMsgTags()),
        //                            NO_CLIENT_COMPUTE).setBmt(&bmt)->execute()->_zi;
        // });
        // cx = f0.get();
        // cy = f1.get();
        // _zi = ring(cx ^ _yi ^ cy);
    }
    return this;
}

// int BoolMutexExecutor::needBmts(int l) {
//     return 2 * BoolAndExecutor::needBmts(l);
// }

BoolMutexExecutor *BoolMutexExecutor::setBmt(Bmt *bmt) {
    _bmt = bmt;
    return this;
}

// BoolMutexExecutor * BoolMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
//     _bmts = bmts;
//     return this;
// }

// int16_t BoolMutexExecutor::needMsgTags() {
//     return static_cast<int16_t>(2 * BoolAndExecutor::needMsgTags());
// }
