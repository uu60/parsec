//
// Created by 杜建璋 on 25-4-18.
//

#include "../../include/intermediate/PipelineBitwiseBmtBatchGenerator.h"

#include "../../include/intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

PipelineBitwiseBmtBatchGenerator *PipelineBitwiseBmtBatchGenerator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    subThreadHandle();

    mainThreadHandle();

    return this;
}

void PipelineBitwiseBmtBatchGenerator::generateRandomAB(std::vector<int64_t> &as, std::vector<int64_t> &bs) {
    int size = Conf::BMT_GEN_BATCH_SIZE;
    as.resize(size);
    bs.resize(size);
    auto bmts = std::vector<BitwiseBmt>(size);

    for (int i = 0; i < size; i++) {
        auto &bmt = bmts[i];
        bmt._a = Math::randInt();
        bmt._b = Math::randInt();
        as[i] = bmt._a;
        bs[i] = bmt._b;
    }
    _bmts.offer(std::move(bmts));
}

void PipelineBitwiseBmtBatchGenerator::compute(int sender, std::vector<int64_t> &as, std::vector<int64_t> &bs) {
    // messages and choices are stored in int64_t
    std::vector<int64_t> ssi, sso;
    std::vector<int64_t> choices;

    int bc = Conf::BMT_GEN_BATCH_SIZE;
    if (Comm::rank() == sender) {
        ssi.resize(bc);
        sso.resize(bc);
        for (int i = 0; i < bc; i++) {
            int64_t random = Math::randInt();
            ssi[i] = random;
            sso[i] = corr(as, i, random);
        }

        otAsync(sender, ssi, sso, choices);
        _ssis.offer(std::move(ssi));
    } else {
        choices.resize(bc);
        for (int i = 0; i < bc; i++) {
            choices[i] = bs[i];
        }
        otAsync(sender, ssi, sso, choices);
    }
}

void PipelineBitwiseBmtBatchGenerator::otAsync(int sender, std::vector<int64_t> &bits0,
                                               std::vector<int64_t> &bits1,
                                               std::vector<int64_t> &choiceBits) {
    if (Comm::rank() == sender) {
        int size = static_cast<int>(bits0.size());
        auto send = std::make_unique<std::vector<int64_t> >(size * 4);

        int64_t ir00 = IntermediateDataSupport::_sRot0->_r0;
        int64_t ir01 = IntermediateDataSupport::_sRot0->_r1;
        int64_t ir10 = IntermediateDataSupport::_sRot1->_r0;
        int64_t ir11 = IntermediateDataSupport::_sRot1->_r1;

        for (int i = 0; i < size; ++i) {
            (*send)[i * 4] = bits0[i] ^ ir00;
            (*send)[i * 4 + 1] = bits1[i] ^ ir01;
            (*send)[i * 4 + 2] = bits0[i] ^ ir10;
            (*send)[i * 4 + 3] = bits1[i] ^ ir11;
        }

        Comm::serverSendAsync(*send, _width, buildTag(_currentMsgTag));
    } else {
        // Do OT async and handle in sub-thread
        int size = static_cast<int>(choiceBits.size());
        // Memory recycle in sub-thread

        HandleData hd;
        hd._recv = new std::vector<int64_t>();
        hd._r = Comm::serverReceiveAsync(*hd._recv, size * 4, _width, buildTag(_currentMsgTag));
        hd._choiceBits = new std::vector(std::move(choiceBits));
        _handle.offer(hd);
    }
}

void PipelineBitwiseBmtBatchGenerator::mainThreadHandle() {
    while (!System::_shutdown) {
        // Generate randoms a and b
        std::vector<int64_t> as, bs;
        generateRandomAB(as, bs);
        compute(0, as, bs);
        compute(1, as, bs);
        ++_currentMsgTag;
    }
}

void PipelineBitwiseBmtBatchGenerator::subThreadHandle() {
    ThreadPoolSupport::submit([this] {
        bool ib0 = IntermediateDataSupport::_rRot0->_b;
        int64_t ir0b = IntermediateDataSupport::_rRot0->_rb;
        int64_t ir1b = IntermediateDataSupport::_rRot1->_rb;
        while (!System::_shutdown) {
            int size = Conf::BMT_GEN_BATCH_SIZE;
            std::vector<int64_t> ssbs(size);

            auto hd = _handle.poll();
            Comm::wait(hd._r);
            auto recv = hd._recv;
            auto choiceBits = hd._choiceBits;

            for (int i = 0; i < size; ++i) {
                int64_t choice_bits = (*choiceBits)[i];

                int64_t mask_sel0 = ib0 ? choice_bits : ~choice_bits;

                int64_t yb00 = (*recv)[i * 4 + 0];
                int64_t yb01 = (*recv)[i * 4 + 1];
                int64_t yb10 = (*recv)[i * 4 + 2];
                int64_t yb11 = (*recv)[i * 4 + 3];

                int64_t yb_sel0 = (yb00 & ~choice_bits) | (yb01 & choice_bits);
                int64_t yb_sel1 = (yb10 & ~choice_bits) | (yb11 & choice_bits);

                int64_t yb_combined = (yb_sel0 & mask_sel0) | (yb_sel1 & ~mask_sel0);

                int64_t xor_val = (ir0b & mask_sel0) | (ir1b & ~mask_sel0);

                ssbs[i] = yb_combined ^ xor_val;
            }

            delete recv;
            delete choiceBits;

            // Compute u and v
            auto ssi = _ssis.poll();
            auto bmts = _bmts.poll();
            for (int i = 0; i < size; ++i) {
                int64_t ui = ssi[i];
                int64_t vi = ssbs[i];
                auto &bmt = bmts[i];
                bmt._c = bmt._a & bmt._b ^ ui ^ vi;
                IntermediateDataSupport::_bitwiseBmtQs[_index]->offer(bmt);
            }
        }
    });
}

int64_t PipelineBitwiseBmtBatchGenerator::corr(std::vector<int64_t> &as, int bmtIdx, int64_t x) {
    return as[bmtIdx] ^ x;
}

PipelineBitwiseBmtBatchGenerator *PipelineBitwiseBmtBatchGenerator::reconstruct(int clientRank) {
    throw std::runtime_error("reconstruct not implemented");
}
