//
// Created by 杜建璋 on 2024/12/1.
//

#include "compute/single/arith/ArithToBoolExecutor.h"

#include "comm/Comm.h"
#include "compute/single/bool/BoolAndExecutor.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

void ArithToBoolExecutor::prepareBmts(BitwiseBmt &b0, BitwiseBmt &b1, BitwiseBmt &b2) const {
    if (_bmts != nullptr) {
        b0 = _bmts->at(0);
        b1 = _bmts->at(1);
        b2 = _bmts->at(2);
    } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        auto bs = IntermediateDataSupport::pollBitwiseBmts(3, _width);
        b0 = bs[0];
        b1 = bs[1];
        b2 = bs[2];
    } else {
        auto bmts = BitwiseBmtBatchGenerator(3, _width, _taskTag, _currentMsgTag).execute()->_bmts;
        b0 = bmts[0];
        b1 = bmts[1];
        b2 = bmts[2];
    }
}

ArithToBoolExecutor *ArithToBoolExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        // bitwise separate xi
        // xi is xored into xi_i and xi_o
        int64_t xi_i = Math::randInt();
        int64_t xi_o = xi_i ^ _xi;
        bool carry_i = false;

        BitwiseBmt b0, b1, b2;
        prepareBmts(b0, b1, b2);

        for (int i = 0; i < _width; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = Comm::rank() == 0 ? &ai : &bi;
            bool *self_o = Comm::rank() == 0 ? &ao : &bo;
            bool *other_i = Comm::rank() == 0 ? &bi : &ai;
            *self_i = (xi_i >> i) & 1;
            *self_o = (xi_o >> i) & 1;
            std::vector self_ov = {static_cast<int64_t>(*self_o)};
            std::vector<int64_t> other_iv;
            Comm::serverSend(self_ov, _width, buildTag(_currentMsgTag));
            Comm::serverReceive(other_iv, _width, buildTag(_currentMsgTag));
            *other_i = other_iv[0];
            this->_zi += static_cast<int64_t>((ai ^ bi) ^ carry_i) << i;

            // Compute carry
            if (i < _width - 1) {
                bool propagate_i = ai ^ bi;

                int cm = _currentMsgTag;
                std::future<int64_t> f;
                bool generate_i;

                if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
                    f = ThreadPoolSupport::submit([&] {
                        auto bmt = b0.extract(i);
                        return BoolAndExecutor(ai, bi, 1, _taskTag, cm, NO_CLIENT_COMPUTE).setBmt(
                            &bmt)->execute()->_zi;
                    });
                } else {
                    auto bmt = b0.extract(i);
                    generate_i = BoolAndExecutor(ai, bi, 1, _taskTag, cm, NO_CLIENT_COMPUTE).setBmt(
                        &bmt)->execute()->_zi;
                }

                _currentMsgTag += BoolAndExecutor::msgTagCount(1);

                auto bmt = b1.extract(i);
                bool tempCarry_i = BoolAndExecutor(propagate_i, carry_i, 1, _taskTag, _currentMsgTag, -1).setBmt(
                    &bmt)->execute()->_zi;

                if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
                    generate_i = f.get();
                }
                bool sum_i = generate_i ^ tempCarry_i;

                bmt = b2.extract(i);
                bool and_i = BoolAndExecutor(generate_i, tempCarry_i, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                        setBmt(&bmt)->execute()->_zi;

                carry_i = sum_i ^ and_i;
            }
        }
        _zi = ring(_zi);
    }

    return this;
}

int ArithToBoolExecutor::msgTagCount(int l) {
    return static_cast<int>(2 * BoolAndExecutor::msgTagCount(l));
}

ArithToBoolExecutor *ArithToBoolExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Bmt size mismatch.");
    }
    _bmts = bmts;
    return this;
}

ArithToBoolExecutor *ArithToBoolExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    BoolExecutor e(_zi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (Comm::rank() == clientRank) {
        _result = e._result;
    }
    return this;
}

int ArithToBoolExecutor::bmtCount(int width) {
    return 3;
}
