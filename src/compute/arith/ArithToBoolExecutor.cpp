//
// Created by 杜建璋 on 2024/12/1.
//

#include "compute/arith/ArithToBoolExecutor.h"

#include "comm/Comm.h"
#include "compute/bool/BoolAndExecutor.h"
#include "intermediate/BmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithToBoolExecutor *ArithToBoolExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        // bitwise separate xi
        // xi is xored into xi_i and xi_o
        int64_t xi_i = Math::randInt();
        int64_t xi_o = xi_i ^ _xi;
        bool carry_i = false;

        for (int i = 0; i < _l; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = Comm::rank() == 0 ? &ai : &bi;
            bool *self_o = Comm::rank() == 0 ? &ao : &bo;
            bool *other_i = Comm::rank() == 0 ? &bi : &ai;
            *self_i = (xi_i >> i) & 1;
            *self_o = (xi_o >> i) & 1;
            std::vector self_ov = {static_cast<int64_t>(*self_o)};
            std::vector<int64_t> other_iv;
            Comm::serverSend(self_ov, buildTag(_currentMsgTag));
            Comm::serverReceive(other_iv, buildTag(_currentMsgTag));
            *other_i = other_iv[0];
            this->_zi += static_cast<int64_t>((ai ^ bi) ^ carry_i) << i;

            // Compute carry
            if (i < _l - 1) {
                bool propagate_i = ai ^ bi;

                std::vector<Bmt> vec0, vec1, vec2;

                // int bmtLimit = Conf::BMT_USAGE_LIMIT;
                if (_bmts != nullptr) {
                    vec0 = {_bmts->at(i * 3)};
                } else if (Conf::INTERM_PREGENERATED) {
                    vec0 = IntermediateDataSupport::pollBmts(1, 1);
                }

                int16_t cm = _currentMsgTag;
                auto f = System::_threadPool.push([&](int) {
                    return BoolAndExecutor(ai, bi, 1, _taskTag, cm, NO_CLIENT_COMPUTE).setBmts(
                        !Conf::INTERM_PREGENERATED ? nullptr : &vec0)->execute()->_zi;
                });

                _currentMsgTag += BoolAndExecutor::needMsgTags();

                // bmtLimit -= 1;
                if (_bmts != nullptr) {
                    vec1 = {_bmts->at(i * 3 + 1)};
                } else if (Conf::INTERM_PREGENERATED) {
                    vec1 = IntermediateDataSupport::pollBmts(1, 1);
                }

                bool tempCarry_i = BoolAndExecutor(propagate_i, carry_i, 1, _taskTag, _currentMsgTag, -1).setBmts(
                            !Conf::INTERM_PREGENERATED ? nullptr : &vec1)->execute()->_zi;
                // bmtLimit -= 1;
                bool generate_i = f.get();
                bool sum_i = generate_i ^ tempCarry_i;

                if (_bmts != nullptr) {
                    vec2 = {_bmts->at(i * 3 + 2)};
                } else if (Conf::INTERM_PREGENERATED) {
                    vec2 = IntermediateDataSupport::pollBmts(1, 1);
                }

                bool and_i = BoolAndExecutor(generate_i, tempCarry_i, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                        setBmts(!Conf::INTERM_PREGENERATED ? nullptr : &vec2)->execute()->_zi;

                carry_i = sum_i ^ and_i;
            }
        }
        _zi = ring(_zi);
    }

    return this;
}

std::string ArithToBoolExecutor::className() const {
    return "ArithToBoolExecutor";
}

int16_t ArithToBoolExecutor::needMsgTags() {
    return static_cast<int16_t>(2 * BoolAndExecutor::needMsgTags());
}

std::pair<int, int> ArithToBoolExecutor::needBmtsWithBits(int l) {
    return {3 * (l - 1), 1};
}

ArithToBoolExecutor *ArithToBoolExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

ArithToBoolExecutor *ArithToBoolExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    BoolExecutor e(_zi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (Comm::rank() == clientRank) {
        _result = e._result;
    }
    return this;
}
