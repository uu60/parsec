//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/arith/ArithMultiplyExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/IComm.h"
#include "utils/Log.h"

ArithMultiplyExecutor *ArithMultiplyExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    // process
    auto msgTags = nextMsgTags(2);
    if (IComm::impl->isServer()) {
        Bmt bmt = _bmt != nullptr ? *_bmt : IntermediateDataSupport::pollBmts(1)[0];
        int64_t ei = ring(_xi - bmt._a);
        int64_t fi = ring(_yi - bmt._b);
        auto f0 = System::_threadPool.push([ei, this, &msgTags](int _) {
            int64_t eo;
            IComm::impl->serverExchange(&ei, &eo, buildTag(msgTags[0]));
            return eo;
        });
        auto f1 = System::_threadPool.push([fi, this, &msgTags](int _) {
            int64_t fo;
            IComm::impl->serverExchange(&fi, &fo, buildTag(msgTags[1]));
            return fo;
        });
        int64_t e = ring(ei + f0.get());
        int64_t f = ring(fi + f1.get());
        _zi = ring(IComm::impl->rank() * e * f + f * bmt._a + e * bmt._b + bmt._c);
    }

    return this;
}

std::string ArithMultiplyExecutor::className() const {
    return "MulExecutor";
}

int16_t ArithMultiplyExecutor::neededMsgTags() {
    return 2;
}

ArithMultiplyExecutor *ArithMultiplyExecutor::setBmt(Bmt *bmt) {
    _bmt = bmt;
    return this;
}
