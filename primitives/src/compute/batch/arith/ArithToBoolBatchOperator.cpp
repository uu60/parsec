
#include "compute/batch/arith/ArithToBoolBatchOperator.h"

#include "comm/Comm.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/single/arith/ArithToBoolOperator.h"
#include "compute/batch/bool/BoolBatchOperator.h"
#include "conf/Conf.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <iterator>
#include <stdexcept>

ArithToBoolBatchOperator::ArithToBoolBatchOperator(std::vector<int64_t> *xs, int width, int taskTag, int msgTagOffset,
                                                   int clientRank)
    : ArithBatchOperator(*xs, width, taskTag, msgTagOffset, clientRank) {
    _xis = new std::vector<int64_t>(std::move(_zis));
    _zis.resize(xs->size(), 0);
}

ArithToBoolBatchOperator::~ArithToBoolBatchOperator() {
    delete _xis;
}

ArithToBoolBatchOperator *ArithToBoolBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    int num = static_cast<int>(_xis->size());
    _zis.resize(num, 0);

    std::vector<BitwiseBmt> backgroundBmts;
    size_t backgroundBmtOffset = 0;
    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
        if (_bmts != nullptr) {
            backgroundBmts = std::move(*_bmts);
        } else {
            backgroundBmts = IntermediateDataSupport::pollBitwiseBmts(bmtCount(num, _width), 64);
        }
        if (backgroundBmts.size() != static_cast<size_t>(bmtCount(num, _width))) {
            throw std::runtime_error("ArithToBoolBatchOperator: Mismatch BMT size.");
        }
    }

    auto nextBackgroundBmts = [&](int count) {
        std::vector<BitwiseBmt> result;
        if (Conf::BMT_METHOD != Conf::BMT_BACKGROUND && Conf::BMT_METHOD != Conf::BMT_PIPELINE) {
            return result;
        }
        const size_t end = backgroundBmtOffset + static_cast<size_t>(count);
        if (end > backgroundBmts.size()) {
            throw std::runtime_error("ArithToBoolBatchOperator: Exhausted assigned BMTs.");
        }
        result.assign(std::make_move_iterator(backgroundBmts.begin() + backgroundBmtOffset),
                      std::make_move_iterator(backgroundBmts.begin() + end));
        backgroundBmtOffset = end;
        return result;
    };

    std::vector<int64_t> xi_i_vec(num), xi_o_vec(num);
    for (int i = 0; i < num; i++) {
        xi_i_vec[i] = Math::randInt();
        xi_o_vec[i] = xi_i_vec[i] ^ (*_xis)[i];
    }

    std::vector<bool> carry_i_vec(num, false);

    for (int bitPos = 0; bitPos < _width; bitPos++) {
        std::vector<bool> ai_vec(num), ao_vec(num), bi_vec(num), bo_vec(num);

        for (int i = 0; i < num; i++) {
            if (Comm::rank() == 0) {
                ai_vec[i] = (xi_i_vec[i] >> bitPos) & 1;
                ao_vec[i] = (xi_o_vec[i] >> bitPos) & 1;
            } else {
                bi_vec[i] = (xi_i_vec[i] >> bitPos) & 1;
                bo_vec[i] = (xi_o_vec[i] >> bitPos) & 1;
            }
        }

        std::vector<int64_t> self_ov(num), other_iv(num);
        for (int i = 0; i < num; i++) {
            if (Comm::rank() == 0) {
                self_ov[i] = static_cast<int64_t>(ao_vec[i]);
            } else {
                self_ov[i] = static_cast<int64_t>(bo_vec[i]);
            }
        }

        auto r0 = Comm::serverSendAsync(self_ov, _width, buildTag(_currentMsgTag));
        auto r1 = Comm::serverReceiveAsync(other_iv, num, _width, buildTag(_currentMsgTag));
        Comm::wait(r0);
        Comm::wait(r1);

        for (int i = 0; i < num; i++) {
            if (Comm::rank() == 0) {
                bi_vec[i] = other_iv[i];
            } else {
                ai_vec[i] = other_iv[i];
            }
        }

        for (int i = 0; i < num; i++) {
            _zis[i] += static_cast<int64_t>((ai_vec[i] ^ bi_vec[i]) ^ carry_i_vec[i]) << bitPos;
        }

        if (bitPos < _width - 1) {
            std::vector<bool> propagate_i_vec(num), generate_i_vec(num), tempCarry_i_vec(num);

            for (int i = 0; i < num; i++) {
                propagate_i_vec[i] = ai_vec[i] ^ bi_vec[i];
            }

            std::vector<int64_t> ai_int(num), bi_int(num);
            for (int i = 0; i < num; i++) {
                ai_int[i] = static_cast<int64_t>(ai_vec[i]);
                bi_int[i] = static_cast<int64_t>(bi_vec[i]);
            }

            BoolAndBatchOperator generateOp(&ai_int, &bi_int, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            auto generateBmts = nextBackgroundBmts(BoolAndBatchOperator::bmtCount(num, 1));
            if (!generateBmts.empty()) generateOp.setBmts(&generateBmts);
            generateOp.execute();

            std::vector<int64_t> propagate_int(num), carry_int(num);
            for (int i = 0; i < num; i++) {
                propagate_int[i] = static_cast<int64_t>(propagate_i_vec[i]);
                carry_int[i] = static_cast<int64_t>(carry_i_vec[i]);
            }

            BoolAndBatchOperator propagateOp(&propagate_int, &carry_int, 1, _taskTag, _currentMsgTag,
                                             NO_CLIENT_COMPUTE);
            auto propagateBmts = nextBackgroundBmts(BoolAndBatchOperator::bmtCount(num, 1));
            if (!propagateBmts.empty()) propagateOp.setBmts(&propagateBmts);
            propagateOp.execute();

            std::vector<int64_t> generate_results = generateOp._zis;
            std::vector<int64_t> tempCarry_results = propagateOp._zis;
            std::vector<int64_t> sum_int(num);

            for (int i = 0; i < num; i++) {
                generate_i_vec[i] = generate_results[i];
                tempCarry_i_vec[i] = tempCarry_results[i];
                sum_int[i] = generate_results[i] ^ tempCarry_results[i];
            }

            BoolAndBatchOperator finalOp(&generate_results, &tempCarry_results, 1, _taskTag, _currentMsgTag,
                                         NO_CLIENT_COMPUTE);
            auto finalBmts = nextBackgroundBmts(BoolAndBatchOperator::bmtCount(num, 1));
            if (!finalBmts.empty()) finalOp.setBmts(&finalBmts);
            finalOp.execute();

            for (int i = 0; i < num; i++) {
                carry_i_vec[i] = sum_int[i] ^ finalOp._zis[i];
            }
        }
    }

    for (int i = 0; i < num; i++) {
        _zis[i] = ring(_zis[i]);
    }

    if ((Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) &&
        backgroundBmtOffset != backgroundBmts.size()) {
        throw std::runtime_error("ArithToBoolBatchOperator: Unused assigned BMTs.");
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

ArithToBoolBatchOperator *ArithToBoolBatchOperator::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    BoolBatchOperator e(_zis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (Comm::rank() == clientRank) {
        _results = e._results;
    }
    return this;
}

int ArithToBoolBatchOperator::tagStride(int width) {
    return BoolAndBatchOperator::tagStride();
}

ArithToBoolBatchOperator *ArithToBoolBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != static_cast<size_t>(bmtCount(static_cast<int>(_xis->size()), _width))) {
        throw std::runtime_error("ArithToBoolBatchOperator: Mismatch BMT size.");
    }
    _bmts = bmts;
    return this;
}

int ArithToBoolBatchOperator::bmtCount(int num, int width) {
    if (width <= 1) {
        return 0;
    }
    return 3 * (width - 1) * BoolAndBatchOperator::bmtCount(num, 1);
}
