//
// Created by 杜建璋 on 2025/1/31.
//

#include "../../include/ot/RandOtBatchOperator.h"

#include "../../include/intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"

RandOtBatchOperator::RandOtBatchOperator(int sender, std::vector<int64_t> *bits0, std::vector<int64_t> *bits1,
                                         std::vector<int64_t> *choiceBits, int taskTag,
                                         int msgTagOffset) : AbstractOtBatchOperator(64, taskTag, msgTagOffset) {
    if (Comm::isClient()) {
        return;
    }

    _doBits = true;
    _isSender = sender == Comm::rank();
    if (_isSender) {
        _ms0 = bits0;
        _ms1 = bits1;
    } else {
        _choiceBits = choiceBits;
    }
}

void RandOtBatchOperator::execute0() {
    if (_isSender) {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_ms0->size());
        auto r0 = Comm::serverReceiveAsync(ks, size, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toSend;
        toSend.resize(size * 2);
        Comm::wait(r0);
        for (int i = 0; i < size; ++i) {
            toSend[i * 2] = (*_ms0)[i] ^ (ks[i] == 0
                                              ? IntermediateDataSupport::_sRot0->_r0
                                              : IntermediateDataSupport::_sRot0->_r1);
            toSend[i * 2 + 1] = (*_ms1)[i] ^ (ks[i] == 0
                                                  ? IntermediateDataSupport::_sRot0->_r1
                                                  : IntermediateDataSupport::_sRot0->_r0);
        }
        auto r1 = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag));
        Comm::wait(r1);
    } else {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_choices->size());
        ks.resize(size);
        for (int i = 0; i < size; ++i) {
            ks[i] = IntermediateDataSupport::_rRot0->_b ^ (*_choices)[i];
        }
        auto r0 = Comm::serverSendAsync(ks, 1, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 2, _width, buildTag(_currentMsgTag));

        _results.resize(size);
        Comm::wait(r1);
        for (int i = 0; i < size; ++i) {
            _results[i] = ring(toRecv[i * 2 + (*_choices)[i]] ^ IntermediateDataSupport::_rRot0->_rb);
        }
        Comm::wait(r0);
    }
}

void RandOtBatchOperator::executeForBits() {
    if (_isSender) {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_ms0->size());
        auto r0 = Comm::serverReceiveAsync(ks, size, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toSend(size * 2);
        Comm::wait(r0);

        int64_t ir0 = IntermediateDataSupport::_sRot0->_r0;
        int64_t ir1 = IntermediateDataSupport::_sRot0->_r1;
        for (int i = 0; i < size; ++i) {
            int64_t ks_i = ks[i];

            int64_t mask0 = (ir0 & ~ks_i) | (ir1 & ks_i);
            int64_t mask1 = (ir1 & ~ks_i) | (ir0 & ks_i);

            toSend[i * 2] = (*_ms0)[i] ^ mask0;
            toSend[i * 2 + 1] = (*_ms1)[i] ^ mask1;
        }
        auto r1 = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag));
        Comm::wait(r1);
    } else {
        std::vector<int64_t> ks;
        int size = static_cast<int>(_choiceBits->size());
        ks.resize(size);

        int64_t ib = IntermediateDataSupport::_rRot0->_b;
        if (ib == 1) {
            ib = -1;
        }

        for (int i = 0; i < size; ++i) {
            ks[i] = ib ^ (*_choiceBits)[i];
        }

        auto r0 = Comm::serverSendAsync(ks, _width, buildTag(_currentMsgTag));

        std::vector<int64_t> toRecv;
        auto r1 = Comm::serverReceiveAsync(toRecv, size * 2, _width, buildTag(_currentMsgTag));

        int64_t rb = IntermediateDataSupport::_rRot0->_rb;
        _results.resize(size);
        Comm::wait(r1);
        for (int i = 0; i < size; ++i) {
            int64_t choice = (*_choiceBits)[i];

            int64_t block0 = toRecv[i * 2];
            int64_t block1 = toRecv[i * 2 + 1];

            int64_t selected = (block0 & ~choice) | (block1 & choice);

            _results[i] = selected ^ rb;
        }
        Comm::wait(r0);
    }
}

void RandOtBatchOperator::executeForBitsSingleTransfer() {
    if (_isSender) {
        int size = static_cast<int>(_ms0->size());
        std::vector<int64_t> toSend(size * 4);

        int64_t ir00 = IntermediateDataSupport::_sRot0->_r0;
        int64_t ir01 = IntermediateDataSupport::_sRot0->_r1;
        int64_t ir10 = IntermediateDataSupport::_sRot1->_r0;
        int64_t ir11 = IntermediateDataSupport::_sRot1->_r1;

        for (int i = 0; i < size; ++i) {
            toSend[i * 4] = (*_ms0)[i] ^ ir00;
            toSend[i * 4 + 1] = (*_ms1)[i] ^ ir01;
            toSend[i * 4 + 2] = (*_ms0)[i] ^ ir10;
            toSend[i * 4 + 3] = (*_ms1)[i] ^ ir11;
        }

        auto r = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag));
        Comm::wait(r);
    } else {
        int size = static_cast<int>(_choiceBits->size());
        std::vector<int64_t> toRecv(size * 4);

        bool ib0 = IntermediateDataSupport::_rRot0->_b;
        int64_t ir0b = IntermediateDataSupport::_rRot0->_rb;
        int64_t ir1b = IntermediateDataSupport::_rRot1->_rb;

        _results.resize(size);

        auto r = Comm::serverReceiveAsync(toRecv, size * 4, _width, buildTag(_currentMsgTag));
        Comm::wait(r);

        for (int i = 0; i < _choiceBits->size(); ++i) {
            int64_t choice_bits = (*_choiceBits)[i];

            int64_t mask_sel0 = ib0 ? choice_bits : ~choice_bits;

            int64_t yb00 = toRecv[i * 4 + 0];
            int64_t yb01 = toRecv[i * 4 + 1];
            int64_t yb10 = toRecv[i * 4 + 2];
            int64_t yb11 = toRecv[i * 4 + 3];

            int64_t yb_sel0 = (yb00 & ~choice_bits) | (yb01 & choice_bits);
            int64_t yb_sel1 = (yb10 & ~choice_bits) | (yb11 & choice_bits);

            int64_t yb_combined = (yb_sel0 & mask_sel0) | (yb_sel1 & ~mask_sel0);

            int64_t xor_val = (ir0b & mask_sel0) | (ir1b & ~mask_sel0);

            _results[i] = yb_combined ^ xor_val;

            /**
             * The following is the same logic but doing in bit loop instead of current bit operations.
             */
            // int64_t e = 0;
            // for (int j = 0; j < _width; j++) {
            //     bool choice = Math::getBit((*_choiceBits)[i], j);
            //     if (choice == ib0) {
            //         int64_t yb = toRecv[i * 4 + choice];
            //         e ^= static_cast<int64_t>(Math::getBit(yb ^ ir0b, j)) << j;
            //     } else {
            //         int64_t yb = toRecv[i * 4 + 2 + choice];
            //         e ^= static_cast<int64_t>(Math::getBit(yb ^ ir1b, j)) << j;
            //     }
            // }
            // _results[i] = e;
        }
    }
}

void RandOtBatchOperator::executeForBitsAsync() {
    if (_isSender) {
        int size = static_cast<int>(_ms0->size());
        std::vector<int64_t> toSend(size * 4);

        int64_t ir00 = IntermediateDataSupport::_sRot0->_r0;
        int64_t ir01 = IntermediateDataSupport::_sRot0->_r1;
        int64_t ir10 = IntermediateDataSupport::_sRot1->_r0;
        int64_t ir11 = IntermediateDataSupport::_sRot1->_r1;

        for (int i = 0; i < size; ++i) {
            toSend[i * 4] = (*_ms0)[i] ^ ir00;
            toSend[i * 4 + 1] = (*_ms1)[i] ^ ir01;
            toSend[i * 4 + 2] = (*_ms0)[i] ^ ir10;
            toSend[i * 4 + 3] = (*_ms1)[i] ^ ir11;
        }

        auto r = Comm::serverSendAsync(toSend, _width, buildTag(_currentMsgTag));
    } else {
        int size = static_cast<int>(_choiceBits->size());
        std::vector<int64_t> toRecv(size * 4);

        bool ib0 = IntermediateDataSupport::_rRot0->_b;
        int64_t ir0b = IntermediateDataSupport::_rRot0->_rb;
        int64_t ir1b = IntermediateDataSupport::_rRot1->_rb;

        _results.resize(size);

        auto r = Comm::serverReceiveAsync(toRecv, size * 4, _width, buildTag(_currentMsgTag));

        ThreadPoolSupport::submit([&] {
            Comm::wait(r);

            std::vector<int64_t> res(size);
            for (int i = 0; i < size; ++i) {
                int64_t choice_bits = (*_choiceBits)[i];

                int64_t mask_sel0 = ib0 ? choice_bits : ~choice_bits;

                int64_t yb00 = toRecv[i * 4 + 0];
                int64_t yb01 = toRecv[i * 4 + 1];
                int64_t yb10 = toRecv[i * 4 + 2];
                int64_t yb11 = toRecv[i * 4 + 3];

                int64_t yb_sel0 = (yb00 & ~choice_bits) | (yb01 & choice_bits);
                int64_t yb_sel1 = (yb10 & ~choice_bits) | (yb11 & choice_bits);

                int64_t yb_combined = (yb_sel0 & mask_sel0) | (yb_sel1 & ~mask_sel0);

                int64_t xor_val = (ir0b & mask_sel0) | (ir1b & ~mask_sel0);

                res[i] = yb_combined ^ xor_val;
            }

            return res;
        });
    }
}

RandOtBatchOperator *RandOtBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_doBits) {
        if (Conf::ENABLE_REDUNDANT_OT) {
            executeForBitsSingleTransfer();
        } else {
            executeForBits();
        }
    } else {
        execute0();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int RandOtBatchOperator::msgTagCount() {
    return 1;
}
