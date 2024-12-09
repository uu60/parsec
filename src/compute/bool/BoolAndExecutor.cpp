//
// Created by 杜建璋 on 2024/11/12.
//

#include "compute/bool/BoolAndExecutor.h"

#include "compute/arith/ArithMulExecutor.h"
#include "comm/IComm.h"

int32_t BoolAndExecutor::_totalMsgTagNum = 0;
int32_t BoolAndExecutor::_currentObjTag = 0;

BoolAndExecutor *BoolAndExecutor::execute() {
    if (IComm::impl->isServer()) {
        std::vector<ArithMulExecutor> executors;
        executors.reserve(_l);

        for (int i = 0; i < _l; i++) {
            executors.emplace_back((_xi >> i) & 1, (_yi >> i) & 1, 1, _objTag, _currentMsgTag, -1);
            _currentMsgTag = static_cast<int8_t>(_currentMsgTag + executors[i].msgNum());
        }

        std::vector<std::future<int64_t> > futures;
        futures.reserve(_l);

        for (int i = 0; i < _l; i++) {
            futures.push_back(System::_threadPool.push([this, &executors, i] (int _) {
                return (executors[i].execute()->_zi) << i;
            }));
        }
        for (auto &f: futures) {
            _zi += f.get();
        }
    }
    return this;
}

std::string BoolAndExecutor::className() const {
    return "BitwiseAndExecutor";
}

int8_t BoolAndExecutor::msgNum(int l) {
    return static_cast<int8_t>(l * 2);
}
