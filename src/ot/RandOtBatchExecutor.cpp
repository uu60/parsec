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
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_isSender) {
        std::vector<int64_t> ks;
        int size = _ms0->size();
        auto r0 = Comm::serverReceiveAsync(ks, size, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toSend;
        toSend.resize(size * 2);
        Comm::wait(r0);
        for (int i = 0; i < size; ++i) {
            toSend[i * 2] = (*_ms0)[i] ^ (ks[i] == 0
                                         ? IntermediateDataSupport::_sRot->_r0
                                         : IntermediateDataSupport::_sRot->_r1);
            toSend[i * 2 + 1] = (*_ms1)[i] ^ (ks[i] == 0
                                         ? IntermediateDataSupport::_sRot->_r1
                                         : IntermediateDataSupport::_sRot->_r0);
        }
        Comm::serverSend(toSend, _width, buildTag(_currentMsgTag));
    } else {
        std::vector<int64_t> ks;
        int size = _choices->size();
        ks.resize(size);
        for (int i = 0; i < size; ++i) {
            ks[i] = IntermediateDataSupport::_rRot->_b ^ (*_choices)[i];
        }
        auto r0 = Comm::serverSendAsync(ks, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 2, _width, buildTag(_currentMsgTag));

        _results.resize(size);
        Comm::wait(r1);
        for (int i = 0; i < size; ++i) {
            _results[i] = ring(toRecv[i * 2 + _choices->at(i)] ^ IntermediateDataSupport::_rRot->_rb);
        }
        Comm::wait(r0);
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int RandOtBatchExecutor::msgTagCount() {
    return 1;
}
