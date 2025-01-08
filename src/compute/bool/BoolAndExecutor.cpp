//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/bool/BoolAndExecutor.h"

#include "compute/arith/ArithMultiplyExecutor.h"
#include "comm/IComm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

BoolAndExecutor *BoolAndExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        std::vector<std::future<int64_t> > futures;
        futures.reserve(_l);

        int64_t ei = 0, fi = 0;
        std::vector<Bmt> bmts;
        bmts.reserve(_l);
        for (int i = 0; i < _l; i++) {
            auto bmt = _bmts == nullptr ? IntermediateDataSupport::pollBmts(1)[0] : (*_bmts)[i];
            bmt._a &= 1;
            bmt._b &= 1;
            bmt._c &= 1;
            ei = Math::changeBit(ei, i, Math::getBit(_xi, i) ^ bmt._a);
            fi = Math::changeBit(fi, i, Math::getBit(_yi, i) ^ bmt._b);
            bmts.emplace_back(bmt);
        }

        auto f0 = System::_threadPool.push([ei, this](int _) {
            int64_t eo;
            IComm::impl->serverSend(&ei, buildTag(_currentMsgTag));
            IComm::impl->serverReceive(&eo, buildTag(_currentMsgTag));
            return eo;
        });
        auto f1 = System::_threadPool.push([fi, this](int _) {
            int64_t fo;
            IComm::impl->serverSend(&fi, buildTag(static_cast<int16_t>(_currentMsgTag + 1)));
            IComm::impl->serverReceive(&fo, buildTag(static_cast<int16_t>(_currentMsgTag + 1)));
            return fo;
        });

        int64_t e = ring(ei ^ f0.get());
        int64_t f = ring(fi ^ f1.get());
        for (int i = 0; i < _l; i++) {
            bool e_bit_i = Math::getBit(e, i);
            bool f_bit_i = Math::getBit(f, i);
            _zi = Math::changeBit(
                _zi, i,
                (IComm::impl->rank() & e_bit_i & f_bit_i) ^ (f_bit_i & bmts[i]._a) ^ (e_bit_i & bmts[i]._b) ^ bmts[i].
                _c);
        }
    }
    return this;
}

std::string BoolAndExecutor::className() const {
    return "BitwiseAndExecutor";
}

int16_t BoolAndExecutor::needsMsgTags(int l) {
    return static_cast<int16_t>(l * 2);
}

int BoolAndExecutor::needsBmts(int l) {
    return l;
}

BoolAndExecutor *BoolAndExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
