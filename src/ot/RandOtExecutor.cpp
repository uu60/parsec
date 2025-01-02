//
// Created by 杜建璋 on 2024/12/29.
//

#include "./ot/RandOtExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

RandOtExecutor *RandOtExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        int64_t k, y0, y1;
        if (_isSender) {
            IComm::impl->serverReceive(&k, buildTag(_currentMsgTag));

            y0 = _m0 ^ (k == 0 ? IntermediateDataSupport::_sRot->_r0 : IntermediateDataSupport::_sRot->_r1);
            y1 = _m1 ^ (k == 0 ? IntermediateDataSupport::_sRot->_r1 : IntermediateDataSupport::_sRot->_r0);

            auto f0 = System::_threadPool.push([this, y0](int _) {
                IComm::impl->serverSend(&y0, buildTag(static_cast<int16_t>(_currentMsgTag + 1)));
            });
            auto f1 = System::_threadPool.push([this, y1](int _) {
                IComm::impl->serverSend(&y1, buildTag(static_cast<int16_t>(_currentMsgTag + 2)));
            });

            f0.wait();
            f1.wait();
        } else {
            k = IntermediateDataSupport::_rRot->_b ^ _choice;
            IComm::impl->serverSend(&k, buildTag(_currentMsgTag));

            IComm::impl->serverReceive(&y0, buildTag(static_cast<int16_t>(_currentMsgTag + 1)));
            IComm::impl->serverReceive(&y1, buildTag(static_cast<int16_t>(_currentMsgTag + 2)));
            _result = (_choice == 0 ? y0 : y1) ^ IntermediateDataSupport::_rRot->_rb;
        }
    }
    return this;
}

int16_t RandOtExecutor::needsMsgTags() {
    return 3;
}

std::string RandOtExecutor::className() const {
    return "RandOtExecutor";
}
