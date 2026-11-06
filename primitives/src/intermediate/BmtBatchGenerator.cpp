#include "intermediate/BmtBatchGenerator.h"

#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "ot/IknpOtBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

namespace parsec {

BmtBatchGenerator::BmtBatchGenerator(int count, int l, int taskTag, int msgTagOffset)
    : AbstractBmtBatchGenerator(count, l, taskTag, msgTagOffset) {}

void BmtBatchGenerator::generateRandomAB() {
    for (auto& b : _bmts) {
        b._a = ring(Math::randInt());
        b._b = ring(Math::randInt());
    }
}

int64_t BmtBatchGenerator::corr(int bmtIdx, int round, int64_t x) const {
    const int64_t factor = Math::changeBit(0, round, true);
    return Math::ringSubtract(Math::ringMultiply(_bmts[bmtIdx]._a, factor, _width), x, _width);
}

BmtBatchGenerator* BmtBatchGenerator::reconstruct(int clientRank) { throw std::runtime_error("Not support."); }

int BmtBatchGenerator::tagStride() { return IknpOtBatchOperator::tagStride() * 2; }

void BmtBatchGenerator::computeMix(int sender) {
    bool isSender = Comm::rank() == sender;

    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    size_t bmtCount = _bmts.size();
    size_t all = _width * bmtCount;
    if (isSender) {
        ss0.reserve(all);
        ss1.reserve(all);
        for (int i = 0; i < bmtCount; i++) {
            for (int j = 0; j < _width; ++j) {
                ss0.push_back(Math::randInt());
                ss1.push_back(corr(i, j, ss0[i * _width + j]));
            }
        }
    } else {
        choices.reserve(all);
        for (int i = 0; i < bmtCount; i++) {
            for (int j = 0; j < _width; ++j) {
                choices.push_back(static_cast<int>((_bmts[i]._b >> j) & 1));
            }
        }
    }

    auto results = IknpOtBatchOperator(sender, &ss0, &ss1, &choices, _width, _taskTag,
                                       _currentMsgTag + sender * IknpOtBatchOperator::tagStride())
                       .execute()
                       ->_results;

    std::vector<int64_t> sums;
    sums.reserve(bmtCount);

    if (isSender) {
        for (int i = 0; i < bmtCount; i++) {
            sums.push_back(0);
            for (int j = 0; j < _width; j++) {
                sums[i] = Math::ringAdd(sums[i], ss0[i * _width + j], _width);
            }
        }
    } else {
        for (int i = 0; i < bmtCount; i++) {
            sums.push_back(0);
            for (int j = 0; j < _width; ++j) {
                int idx = i * _width + j;
                int64_t temp = results[idx];
                if (choices[idx] == 0) {
                    temp = Math::ringSubtract(0, temp, _width);
                }
                sums[i] = Math::ringAdd(sums[i], temp, _width);
            }
        }
    }

    std::vector<int64_t>* mix = sender == 0 ? &_usi : &_vsi;
    mix->reserve(bmtCount);
    for (int i = 0; i < bmtCount; i++) {
        mix->push_back(sums[i]);
    }
}

void BmtBatchGenerator::computeC() {
    for (int i = 0; i < _bmts.size(); i++) {
        const int64_t product = Math::ringMultiply(_bmts[i]._a, _bmts[i]._b, _width);
        _bmts[i]._c = Math::ringAdd(product, Math::ringAdd(_usi[i], _vsi[i], _width), _width);
    }
}

BmtBatchGenerator* BmtBatchGenerator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    generateRandomAB();
    if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
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
