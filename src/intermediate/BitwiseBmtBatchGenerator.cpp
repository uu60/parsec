//
// Created by 杜建璋 on 2025/2/19.
//

#include "intermediate/BitwiseBmtBatchGenerator.h"

#include "conf/Conf.h"
#include "ot/RandOtBatchExecutor.h"
#include "ot/RandOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"

BitwiseBmtBatchGenerator::BitwiseBmtBatchGenerator(int count, int l, int taskTag,
                                                   int msgTagOffset) : AbstractBmtBatchGenerator(count,
    l, taskTag, msgTagOffset) {
}

BitwiseBmtBatchGenerator *BitwiseBmtBatchGenerator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    generateRandomAB();

    if (Conf::INTRA_OPERATOR_PARALLELISM) {
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

void BitwiseBmtBatchGenerator::generateRandomAB() {
    for (auto &b: _bmts) {
        b._a = ring(Math::randInt());
        b._b = ring(Math::randInt());
    }
}

void BitwiseBmtBatchGenerator::computeMix(int sender) {
    bool isSender = Comm::rank() == sender;

    // messages and choices are stored in int64_t
    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    size_t bmtCount = _bmts.size();
    size_t all = _width * bmtCount;
    if (isSender) {
        ss0.reserve(all);
        ss1.reserve(all);
        for (int i = 0; i < bmtCount; i++) {
            for (int j = 0; j < _width; ++j) {
                int64_t bit = Math::randInt(0, 1);
                ss0.push_back(bit);
                ss1.push_back(corr(i, j, bit));
            }
        }
    } else {
        choices.reserve(all);
        for (int i = 0; i < bmtCount; i++) {
            for (int j = 0; j < _width; ++j) {
                choices.push_back(Math::getBit(_bmts[i]._b, j));
            }
        }
    }

    auto results = RandOtBatchExecutor(sender, &ss0, &ss1, &choices, _width, _taskTag,
                          _currentMsgTag + sender * RandOtBatchExecutor::msgTagCount()).execute()->_results;

    std::vector<int64_t> sums;
    sums.reserve(bmtCount);

    if (isSender) {
        for (int i = 0; i < bmtCount; i++) {
            sums.push_back(0);
            for (int j = 0; j < _width; ++j) {
                sums[i] += ss0[i * _width + j] << j;
            }
        }
    } else {
        for (int i = 0; i < bmtCount; i++) {
            sums.push_back(0);
            for (int j = 0; j < _width; ++j) {
                sums[i] += results[i * _width + j] << j;
            }
        }
    }

    std::vector<int64_t> *mix = sender == 0 ? &_usi : &_vsi;
    mix->reserve(bmtCount);
    for (int i = 0; i < bmtCount; i++) {
        mix->push_back(ring(sums[i]));
    }
}

void BitwiseBmtBatchGenerator::computeC() {
    for (int i = 0; i < _bmts.size(); i++) {
        _bmts[i]._c = ring(_bmts[i]._a & _bmts[i]._b ^ _usi[i] ^ _vsi[i]);
    }
}

int64_t BitwiseBmtBatchGenerator::corr(int bmtIdx, int round, int64_t x) const {
    return (Math::getBit(_bmts[bmtIdx]._a, round) - x) & 1;
}

AbstractSecureExecutor *BitwiseBmtBatchGenerator::reconstruct(int clientRank) {
    throw std::runtime_error("Not support.");
}

int BitwiseBmtBatchGenerator::msgTagCount(int bmtCount, int width) {
    return static_cast<int>(2 * width * bmtCount * RandOtExecutor::msgTagCount(width));
}
