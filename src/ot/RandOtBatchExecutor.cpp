//
// Created by 杜建璋 on 2025/1/31.
//

#include "../../include/ot/RandOtBatchExecutor.h"

#include "../../include/intermediate/IntermediateDataSupport.h"

RandOtBatchExecutor *RandOtBatchExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_isSender) {
        std::vector<int64_t> ks;
        ks.reserve(_ms0->size());
        Comm::serverReceive(ks, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toSend;
        toSend.reserve(_ms0->size() * 2);
        for (int i = 0; i < _ms0->size(); ++i) {
            auto y0 = _ms0->at(i) ^ (ks[i] == 0
                                         ? IntermediateDataSupport::_sRot->_r0
                                         : IntermediateDataSupport::_sRot->_r1);
            auto y1 = _ms1->at(i) ^ (ks[i] == 0
                                         ? IntermediateDataSupport::_sRot->_r1
                                         : IntermediateDataSupport::_sRot->_r0);
            toSend.push_back(y0);
            toSend.push_back(y1);
        }
        Comm::serverSend(toSend, _width, buildTag(_currentMsgTag));
    } else {
        std::vector<int64_t> ks;
        ks.reserve(_choices->size());
        for (int _choice : *_choices) {
            ks.push_back(IntermediateDataSupport::_rRot->_b ^ _choice);
        }
        Comm::serverSend(ks, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        Comm::serverReceive(toRecv, _width, buildTag(_currentMsgTag));

        _results.reserve(toRecv.size() / 2);
        for (int i = 0; i < _choices->size(); ++i) {
            _results.push_back(ring(toRecv[i * 2 + _choices->at(i)] ^ IntermediateDataSupport::_rRot->_rb));
        }
    }

    if (Conf::CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int RandOtBatchExecutor::msgTagCount() {
    return 1;
}
