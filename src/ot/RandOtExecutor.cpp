//
// Created by 杜建璋 on 2024/12/29.
//

#include "./ot/RandOtExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

RandOtExecutor *RandOtExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        int64_t k, y0, y1;
        if (_isSender) {
            std::vector<int64_t> temp;
            Comm::serverReceive(temp, buildTag(_currentMsgTag));
            k = temp[0];

            y0 = _m0 ^ (k == 0 ? IntermediateDataSupport::_sRot->_r0 : IntermediateDataSupport::_sRot->_r1);
            y1 = _m1 ^ (k == 0 ? IntermediateDataSupport::_sRot->_r1 : IntermediateDataSupport::_sRot->_r0);

            std::vector y01 = {y0, y1};
            Comm::serverSend(y01, buildTag(_currentMsgTag));
        } else {
            k = IntermediateDataSupport::_rRot->_b ^ _choice;

            std::vector kv = {k};
            Comm::serverSend(kv, buildTag(_currentMsgTag));

            std::vector<int64_t> y01;
            Comm::serverReceive(y01, buildTag(_currentMsgTag));

            _result = y01[_choice] ^ IntermediateDataSupport::_rRot->_rb;
        }
    }
    return this;
}

int16_t RandOtExecutor::needMsgTags() {
    return 1;
}

std::string RandOtExecutor::className() const {
    return "RandOtExecutor";
}
