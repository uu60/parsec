//
// Created by 杜建璋 on 2024/12/1.
//

#include "compute/arith/ArithToBoolExecutor.h"

#include "comm/IComm.h"
#include "compute/bool/BoolAndExecutor.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithToBoolExecutor *ArithToBoolExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        // bitwise separate xi
        // xi is xored into xi_i and xi_o
        int64_t xi_i = Math::randInt();
        int64_t xi_o = xi_i ^ _xi;
        bool carry_i = false;

        for (int i = 0; i < _l; i++) {
            bool ai, ao, bi, bo;
            bool *self_i = IComm::impl->rank() == 0 ? &ai : &bi;
            bool *self_o = IComm::impl->rank() == 0 ? &ao : &bo;
            bool *other_i = IComm::impl->rank() == 0 ? &bi : &ai;
            *self_i = (xi_i >> i) & 1;
            *self_o = (xi_o >> i) & 1;
            IComm::impl->serverSend(self_o, buildTag(_currentMsgTag));
            IComm::impl->serverReceive(other_i, buildTag(_currentMsgTag++));
            this->_zi += ((ai ^ bi) ^ carry_i) << i;

            // Compute carry
            if (i < _l - 1) {
                bool propagate_i = ai ^ bi;

                auto vec0 = _bmts == nullptr ? IntermediateDataSupport::pollBmts(1) : std::vector<Bmt>{(*_bmts)[i * 3]};
                auto vec1 = _bmts == nullptr
                                ? IntermediateDataSupport::pollBmts(1)
                                : std::vector<Bmt>{(*_bmts)[i * 3 + 1]};
                auto vec2 = _bmts == nullptr ? IntermediateDataSupport::pollBmts(1) : std::vector<Bmt>{(*_bmts)[i * 3 + 2]};

                auto f0 = System::_threadPool.push([ai, bi, this, &vec0](int _) {
                    return BoolAndExecutor(ai, bi, 1, _taskTag, _currentMsgTag, -1).setBmts(&vec0)->execute()->_zi;
                });
                auto f1 = System::_threadPool.push([propagate_i, carry_i, this, &vec1](int _) {
                    return BoolAndExecutor(propagate_i, carry_i, 1, _taskTag,
                                           static_cast<int16_t>(_currentMsgTag + BoolAndExecutor::needsMsgTags(1)),
                                           -1).setBmts(&vec1)->execute()->_zi;
                });
                bool generate_i = f0.get();
                bool tempCarry_i = f1.get();
                bool sum_i = generate_i ^ tempCarry_i;
                bool and_i = BoolAndExecutor(generate_i, tempCarry_i, 1, _taskTag,
                                             static_cast<int16_t>(
                                                 _currentMsgTag + 2 * BoolAndExecutor::needsMsgTags(1)),
                                             -1).setBmts(&vec2)->execute()->_zi;

                carry_i = sum_i ^ and_i;
                _currentMsgTag = static_cast<int16_t>(_currentMsgTag + 3 * BoolAndExecutor::needsMsgTags(1));
            }
        }

        _zi = ring(_zi);
    }

    return this;
}

std::string ArithToBoolExecutor::className() const {
    return "ArithToBoolExecutor";
}

int16_t ArithToBoolExecutor::needsMsgTags(int l) {
    return static_cast<int16_t>(1 + (l - 1) * (1 + 3 * BoolAndExecutor::needsMsgTags(1)));
}

int ArithToBoolExecutor::needsBmts(int l) {
    return 3 * (l - 1);
}

ArithToBoolExecutor *ArithToBoolExecutor::setBmts(std::vector<Bmt> *bmts) {
    _bmts = bmts;
    return this;
}

ArithToBoolExecutor *ArithToBoolExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    BoolExecutor e(_zi, _l, _taskTag, _currentMsgTag, -1);
    e.reconstruct(clientRank);
    if (IComm::impl->rank() == clientRank) {
        _result = e._result;
    }
    return this;
}
