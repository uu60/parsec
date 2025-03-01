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
            Comm::serverReceive(k, _width, buildTag(_currentMsgTag));

            y0 = _m0 ^ (k == 0 ? IntermediateDataSupport::_sRot->_r0 : IntermediateDataSupport::_sRot->_r1);
            y1 = _m1 ^ (k == 0 ? IntermediateDataSupport::_sRot->_r1 : IntermediateDataSupport::_sRot->_r0);

            std::vector y01 = {y0, y1};
            Comm::serverSend(y01, _width, buildTag(_currentMsgTag));
        } else {
            k = IntermediateDataSupport::_rRot->_b ^ _choice;
            Comm::serverSend(k, _width, buildTag(_currentMsgTag));

            std::vector<int64_t> y01;
            Comm::serverReceive(y01, _width, buildTag(_currentMsgTag));

            _result = ring(y01[_choice] ^ IntermediateDataSupport::_rRot->_rb);
        }
    }
    return this;
}

int16_t RandOtExecutor::msgTagCount(int width) {
    return 1;
}
