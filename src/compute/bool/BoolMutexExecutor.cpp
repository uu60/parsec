//
// Created by 杜建璋 on 2025/1/8.
//

#include "compute/bool/BoolMutexExecutor.h"

#include "intermediate/IntermediateDataSupport.h"

BoolMutexExecutor::BoolMutexExecutor(int64_t x, int64_t y, bool cond, int l, int16_t taskTag, int16_t msgTagOffset,
                                     int clientRank) : BoolExecutor(x, y, l, taskTag, msgTagOffset, clientRank) {
    _cond_i = BoolExecutor(cond, 1, _taskTag, _currentMsgTag, clientRank)._zi;
}

BoolMutexExecutor * BoolMutexExecutor::execute() {
    // if (IComm::impl->isServer()) {
    //     auto bmts = _bmts == nullptr ? IntermediateDataSupport::pollBmts(2) : *_bmts;
    //     int64_t cx, cy;
    //     auto f0 = System::_threadPool.push([this, &bmts](int _) {
    //         auto mul0 = ArithMultiplyExecutor(_cond_i, _xi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    //         return mul0.setBmt(&bmts[0])->execute()->_zi;
    //     });
    //     auto f1 = System::_threadPool.push([this, &bmts](int _) {
    //         auto mul1 = ArithMultiplyExecutor(_cond_i, _yi, _l, _taskTag,
    //                                      static_cast<int16_t>(_currentMsgTag + ArithMultiplyExecutor::needsMsgTags()), NO_CLIENT_COMPUTE);
    //         return mul1.setBmt(&bmts[1])->execute()->_zi;
    //     });
    //     cx = f0.get();
    //     cy = f1.get();
    //     _zi = ring(cx + _yi - cy);
    //
    //     Bmt bmt = _bmt != nullptr ? *_bmt : IntermediateDataSupport::pollBmts(1)[0];
    //     int64_t ei = ring(_xi - bmt._a);
    //     int64_t fi = ring(_yi - bmt._b);
    //     auto f0 = System::_threadPool.push([ei, this](int _) {
    //         int64_t eo;
    //         IComm::impl->serverSend(&ei, buildTag(_currentMsgTag));
    //         IComm::impl->serverReceive(&eo, buildTag(_currentMsgTag));
    //         return eo;
    //     });
    //     auto f1 = System::_threadPool.push([fi, this](int _) {
    //         int64_t fo;
    //         IComm::impl->serverSend(&fi, buildTag(static_cast<int16_t>(_currentMsgTag + 1)));
    //         IComm::impl->serverReceive(&fo, buildTag(static_cast<int16_t>(_currentMsgTag + 1)));
    //         return fo;
    //     });
    //     int64_t e = ring(ei + f0.get());
    //     int64_t f = ring(fi + f1.get());
    //     _zi = ring(IComm::impl->rank() * e * f + f * bmt._a + e * bmt._b + bmt._c);
    // }
    // return this;
}

BoolMutexExecutor * BoolMutexExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
