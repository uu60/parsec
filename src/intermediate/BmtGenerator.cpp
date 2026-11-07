#include "parsec/intermediate/BmtGenerator.h"

#include "parsec/comm/Comm.h"
#include "parsec/conf/Conf.h"
#include "parsec/ot/IknpOtBatchOperator.h"
#include "parsec/parallel/ThreadPoolSupport.h"
#include "parsec/utils/Log.h"
#include "parsec/utils/Math.h"

namespace parsec {

void BmtGenerator::generateRandomAB() {
    _bmt._a = ring(Math::randBits());
    _bmt._b = ring(Math::randBits());
}

int64_t BmtGenerator::corr(int i, int64_t x) const {
    const int64_t factor = Math::changeBit(0, i, true);
    return Math::ringSubtract(Math::ringMultiply(_bmt._a, factor, _width), x, _width);
}

BmtGenerator* BmtGenerator::reconstruct(int clientRank) { throw std::runtime_error("Not support."); }

int BmtGenerator::tagStride(int width) { return 2 * IknpOtBatchOperator::tagStride(); }

void BmtGenerator::computeMix(int sender) {
    int64_t sum = 0;
    bool isSender = Comm::rank() == sender;

    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    if (isSender) {
        ss0.reserve(_width);
        ss1.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            ss0.push_back(Math::randBits());
            ss1.push_back(corr(i, ss0[i]));
        }
    } else {
        choices.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            choices.push_back(static_cast<int>((_bmt._b >> i) & 1));
        }
    }

    auto results = IknpOtBatchOperator(sender, &ss0, &ss1, &choices, _width, _taskTag,
                                       _currentMsgTag + sender * IknpOtBatchOperator::tagStride())
                       .execute()
                       ->_results;

    if (isSender) {
        for (int i = 0; i < ss0.size(); ++i) {
            sum = Math::ringAdd(sum, ss0[i], _width);
        }
    } else {
        for (int i = 0; i < choices.size(); ++i) {
            int64_t temp = results[i];
            if (choices[i] == 0) {
                temp = Math::ringSubtract(0, temp, _width);
            }
            sum = Math::ringAdd(sum, temp, _width);
        }
    }

    if (sender == 0) {
        _ui = sum;
    } else {
        _vi = sum;
    }
}

void BmtGenerator::computeC() {
    const int64_t product = Math::ringMultiply(_bmt._a, _bmt._b, _width);
    _bmt._c = Math::ringAdd(product, Math::ringAdd(_ui, _vi, _width), _width);
}

BmtGenerator* BmtGenerator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    generateRandomAB();

    if (!Conf::DISABLE_MULTI_THREAD && Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
        auto f = ThreadPoolSupport::submit([&] { computeMix(0); });
        computeMix(1);
        f.wait();
    } else {
        computeMix(0);
        computeMix(1);
    }
    computeC();

    return this;
}

} // namespace parsec
