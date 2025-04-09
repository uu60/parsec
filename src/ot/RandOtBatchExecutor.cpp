//
// Created by 杜建璋 on 2025/1/31.
//

#include "../../include/ot/RandOtBatchExecutor.h"

#include "../../include/intermediate/IntermediateDataSupport.h"

RandOtBatchExecutor::RandOtBatchExecutor(int sender, std::vector<int64_t> *bits0, std::vector<int64_t> *bits1,
                                         std::vector<int64_t> *choiceBits, int64_t totalBits, int taskTag,
                                         int msgTagOffset) : AbstractOtBatchExecutor(64, taskTag, msgTagOffset) {
    if (Comm::isClient()) {
        return;
    }

    _doBits = true;
    _totalBits = totalBits;
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _ms0 = bits0;
        _ms1 = bits1;
    } else {
        _choiceBits = choiceBits;
    }
}

void RandOtBatchExecutor::execute0() {
    if (_isSender) {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_ms0->size());
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
        int size = static_cast<int>(_choices->size());
        ks.resize(size);
        for (int i = 0; i < size; ++i) {
            ks[i] = IntermediateDataSupport::_rRot->_b ^ (*_choices)[i];
        }
        auto r0 = Comm::serverSendAsync(ks, 1, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 2, _width, buildTag(_currentMsgTag));

        _results.resize(size);
        Comm::wait(r1);
        for (int i = 0; i < size; ++i) {
            _results[i] = ring(toRecv[i * 2 + (*_choices)[i]] ^ IntermediateDataSupport::_rRot->_rb);
        }
        Comm::wait(r0);
    }
}

void RandOtBatchExecutor::executeForBits() {
    if (_isSender) {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_ms0->size());
        auto r0 = Comm::serverReceiveAsync(ks, size, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toSend(size * 2);
        Comm::wait(r0);

        for (int i = 0; i < size; ++i) {
            int64_t ks_i = ks[i];
            int64_t ir0 = IntermediateDataSupport::_sRot->_r0;
            int64_t ir1 = IntermediateDataSupport::_sRot->_r1;

            int64_t mask0 = (ir0 & ~ks_i) | (ir1 & ks_i);
            int64_t mask1 = (ir1 & ~ks_i) | (ir0 & ks_i);

            toSend[i * 2] = (*_ms0)[i] ^ mask0;
            toSend[i * 2 + 1] = (*_ms1)[i] ^ mask1;
        }
        Comm::serverSend(toSend, _width, buildTag(_currentMsgTag));
    } else {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_choiceBits->size());
        ks.resize(size);

        int64_t ib = IntermediateDataSupport::_rRot->_b;
        if (ib == 1) {
            ib = -1;
        }

        for (int i = 0; i < size; ++i) {
            ks[i] = ib ^ (*_choiceBits)[i];
        }

        auto r0 = Comm::serverSendAsync(ks, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 2, _width, buildTag(_currentMsgTag));

        _results.resize(size);
        Comm::wait(r1);
        for (int i = 0; i < size; ++i) {
            int64_t choice = (*_choiceBits)[i];
            int64_t rb = IntermediateDataSupport::_rRot->_rb;

            int64_t block0 = toRecv[i * 2];
            int64_t block1 = toRecv[i * 2 + 1];

            int64_t selected = (block0 & ~choice) | (block1 & choice);

            _results[i] = selected ^ rb;
        }
        Comm::wait(r0);
    }
}

RandOtBatchExecutor *RandOtBatchExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_doBits) {
        executeForBits();
    } else {
        execute0();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int RandOtBatchExecutor::msgTagCount() {
    return 1;
}

