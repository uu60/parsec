//
// Created by 杜建璋 on 2024/7/13.
//

#include "compute/arith/ArithMulExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/IComm.h"

ArithMulExecutor* ArithMulExecutor::execute() {
    auto bmt = IntermediateDataSupport::pollBmts(1)[0];

    // process
    if (IComm::impl->isServer()) {
        int64_t ei = ring(_xi - bmt._a);
        int64_t fi = ring(_yi - bmt._b);
        auto msgTags = nextMsgTags(2);
        auto f0 = System::_threadPool.push([ei, this, &msgTags] (int _) {
            int64_t eo;
            IComm::impl->serverExchange(&ei, &eo, buildTag(msgTags[0]));
            return eo;
        });
        auto f1 = System::_threadPool.push([fi, this, &msgTags] (int _) {
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

std::string ArithMulExecutor::className() const {
    return "MulExecutor";
}

int8_t ArithMulExecutor::msgNum() {
    return 2;
}
