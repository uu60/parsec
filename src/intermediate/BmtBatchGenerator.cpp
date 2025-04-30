//
// Created by 杜建璋 on 2025/2/19.
//

#include "intermediate/BmtBatchGenerator.h"

#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "ot/RandOtBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

BmtBatchGenerator::BmtBatchGenerator(int count, int l, int taskTag, int msgTagOffset) : AbstractBmtBatchGenerator(
    count, l, taskTag, msgTagOffset) {
}

void BmtBatchGenerator::generateRandomAB() {
    for (auto &b: _bmts) {
        b._a = ring(Math::randInt());
        b._b = ring(Math::randInt());
    }
}

int64_t BmtBatchGenerator::corr(int bmtIdx, int round, int64_t x) const {
    return ring((_bmts[bmtIdx]._a << round) - x);
}

BmtBatchGenerator *BmtBatchGenerator::reconstruct(int clientRank) {
    throw std::runtime_error("Not support.");
}

int BmtBatchGenerator::msgTagCount(int bmtCount, int width) {
    return static_cast<int>(2 * width * bmtCount * RandOtOperator::msgTagCount(width));
}

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
                ss1.push_back(corr(i, j, ss0[i]));
            }
        }
    } else {
        choices.reserve(all);
        for (int i = 0; i < bmtCount; i++) {
            for (int j = 0; j < _width; ++j) {
                choices.push_back(static_cast<int>((_bmts[i]._b >> i) & 1));
            }
        }
    }

    auto results = RandOtBatchOperator(sender, &ss0, &ss1, &choices, _width, _taskTag,
                                       _currentMsgTag + sender * RandOtBatchOperator::msgTagCount()).execute()->
            _results;

    std::vector<int64_t> sums;
    sums.reserve(bmtCount);

    if (isSender) {
        for (int i = 0; i < bmtCount; i++) {
            sums.push_back(0);
            for (int j = 0; j < _width; j++) {
                sums[i] += ss0[i * _width + j];
            }
        }
    } else {
        for (int i = 0; i < bmtCount; i++) {
            sums.push_back(0);
            for (int j = 0; j < _width; ++j) {
                int idx = i * _width + j;
                int64_t temp = _results[idx];
                if (choices[idx] == 0) {
                    temp = -temp;
                }
                sums[i] += temp;
            }
        }
    }

    std::vector<int64_t> *mix = sender == 0 ? &_usi : &_vsi;
    mix->reserve(bmtCount);
    for (int i = 0; i < bmtCount; i++) {
        mix->push_back(ring(sums[i]));
    }
}

void BmtBatchGenerator::computeC() {
    for (int i = 0; i < _bmts.size(); i++) {
        _bmts[i]._c = ring(_bmts[i]._a * _bmts[i]._b + _usi[i] + _vsi[i]);
    }
}

BmtBatchGenerator *BmtBatchGenerator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    generateRandomAB();
    if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
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
    return this;
}
