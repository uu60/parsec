//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/bool/BoolAndExecutor.h"

#include "compute/arith/ArithMulExecutor.h"
#include "comm/IComm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

BoolAndExecutor *BoolAndExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        std::vector<std::future<int64_t> > futures;
        futures.reserve(_l);

        for (int i = 0; i < _l; i++) {
            futures.push_back(System::_threadPool.push([this, i] (int _) {
                ArithMulExecutor e((_xi >> i) & 1, (_yi >> i) & 1, 1, _objTag, static_cast<int16_t>(_currentMsgTag + ArithMulExecutor::neededMsgTags() * i), -1);
                return (e.execute()->_zi) << i;
            }));
        }
        for (auto &f : futures) {
            _zi += f.get();
        }
        _currentMsgTag = static_cast<int16_t>(_currentMsgTag + ArithMulExecutor::neededMsgTags() * _l);
        _zi = ring(_zi);
    }
    return this;
}

std::string BoolAndExecutor::className() const {
    return "BitwiseAndExecutor";
}

int16_t BoolAndExecutor::neededMsgTags(int l) {
    return static_cast<int16_t>(l * 2);
}
