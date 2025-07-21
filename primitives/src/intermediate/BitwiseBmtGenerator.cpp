//
// Created by 杜建璋 on 2025/2/1.
//

#include "../../include/intermediate/BitwiseBmtGenerator.h"
#include "../../include/ot/RandOtBatchOperator.h"
#include "../../include/utils/Math.h"
#include "conf/Conf.h"
#include "ot/RandOtOperator.h"
#include "parallel/ThreadPoolSupport.h"

BitwiseBmtGenerator *BitwiseBmtGenerator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    generateRandomAB();
    if (!Conf::DISABLE_MULTI_THREAD && Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
        auto f = ThreadPoolSupport::submit([&] {
            computeMix(0);
        });
        computeMix(1);
        f.wait();
    } else {
        computeMix(0);
        computeMix(1);
    }
    computeC();

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }
    return this;
}

void BitwiseBmtGenerator::generateRandomAB() {
    _bmt._a = ring(Math::randInt());
    _bmt._b = ring(Math::randInt());
}

void BitwiseBmtGenerator::computeMix(int sender) {
    // atomic integer needed for multiple-thread computation
    int64_t sum = 0;
    bool isSender = Comm::rank() == sender;

    // messages and choices are stored in int64_t
    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    if (isSender) {
        ss0.reserve(_width);
        ss1.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            int64_t bit = Math::randInt(0, 1);
            ss0.push_back(bit);
            ss1.push_back(corr(i, bit));
        }
    } else {
        choices.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            choices.push_back(Math::getBit(_bmt._b, i));
        }
    }

    auto s = Math::randInt();
    auto results = RandOtBatchOperator(sender, &ss0, &ss1, &choices, 1, _taskTag,
                                       _currentMsgTag + sender * RandOtBatchOperator::tagStride()).execute()->
            _results;

    if (isSender) {
        for (int i = 0; i < _width; ++i) {
            sum += ss0[i] << i;
        }
    } else {
        for (int i = 0; i < _width; ++i) {
            sum += results[i] << i;
        }
    }

    if (sender == 0) {
        _ui = ring(sum);
    } else {
        _vi = ring(sum);
    }
}

void BitwiseBmtGenerator::computeC() {
    _bmt._c = ring(_bmt._a & _bmt._b ^ _ui ^ _vi);
}

int64_t BitwiseBmtGenerator::corr(int i, int64_t x) const {
    return (Math::getBit(_bmt._a, i) - x) & 1;
}

SecureOperator *BitwiseBmtGenerator::reconstruct(int clientRank) {
    throw std::runtime_error("Not support.");
}

int BitwiseBmtGenerator::tagStride(int width) {
    return 2 * RandOtBatchOperator::tagStride();
}
