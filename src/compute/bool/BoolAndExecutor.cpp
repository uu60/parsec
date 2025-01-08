//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/bool/BoolAndExecutor.h"

#include "compute/arith/ArithMultiplyExecutor.h"
#include "comm/IComm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

BoolAndExecutor *BoolAndExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        std::vector<std::future<int64_t> > futures;
        futures.reserve(_l);

        for (int i = 0; i < _l; i++) {
            auto bmt = _bmts == nullptr ? IntermediateDataSupport::pollBmts(1)[0] : (*_bmts)[i];
            futures.push_back(System::_threadPool.push([this, i, bmt] (int _) {
                Bmt copy = bmt;
                ArithMultiplyExecutor e((_xi >> i) & 1, (_yi >> i) & 1, 1, _taskTag, static_cast<int16_t>
                    (_currentMsgTag + ArithMultiplyExecutor::needsMsgTags() * i), NO_CLIENT_COMPUTE);
                e.setBmt(&copy);
                return (e.execute()->_zi) << i;
            }));
        }
        for (auto &f : futures) {
            _zi += f.get();
        }
        _zi = ring(_zi);
    }
    return this;
}

std::string BoolAndExecutor::className() const {
    return "BitwiseAndExecutor";
}

int16_t BoolAndExecutor::needsMsgTags(int l) {
    return static_cast<int16_t>(l * 2);
}

BoolAndExecutor * BoolAndExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}
