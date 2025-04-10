//
// Created by 杜建璋 on 2025/2/19.
//

#include "intermediate/BitwiseBmtBatchGenerator.h"

#include "conf/Conf.h"
#include "ot/RandOtBatchExecutor.h"
#include "ot/RandOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"

BitwiseBmtBatchGenerator::BitwiseBmtBatchGenerator(int count, int width, int taskTag,
                                                   int msgTagOffset) : AbstractBmtBatchGenerator(count,
                                                                                                 width, taskTag, msgTagOffset) {
    if (Comm::isClient()) {
        return;
    }
    _totalBits = count * width;
    _bc = static_cast<int>((_totalBits + 63) / 64);
    _bmts.resize(_bc);
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

void BitwiseBmtBatchGenerator::generateRandomAB() {
    for (auto &b: _bmts) {
        b._a = Math::randInt();
        b._b = Math::randInt();
    }
}

void BitwiseBmtBatchGenerator::computeMix(int sender) {
    bool isSender = Comm::rank() == sender;

    // messages and choices are stored in int64_t
    std::vector<int64_t> ss0, ss1;
    std::vector<int64_t> choices;

    if (isSender) {
        ss0.resize(_bc);
        ss1.resize(_bc);
        for (int i = 0; i < _bc; i++) {
            int64_t random = Math::randInt();
            ss0[i] = random;
            ss1[i] = corr(i, random);
        }
    } else {
        choices.resize(_bc);
        for (int i = 0; i < _bc; i++) {
            choices[i] = _bmts[i]._b;
        }
    }

    auto results = RandOtBatchExecutor(sender, &ss0, &ss1, &choices, _totalBits, _taskTag,
                          _currentMsgTag + sender * RandOtBatchExecutor::msgTagCount()).execute()->_results;

    std::vector<int64_t> *mix = sender == 0 ? &_usi : &_vsi;
    mix->resize(_bc);
    for (int i = 0; i < _bc; i++) {
        (*mix)[i] = isSender ? ss0[i] : results[i];
    }
}

void BitwiseBmtBatchGenerator::computeC() {
    for (int i = 0; i < _bmts.size(); i++) {
        auto &bmt = _bmts[i];
        bmt._c = bmt._a & bmt._b ^ _usi[i] ^ _vsi[i];
    }
}

int64_t BitwiseBmtBatchGenerator::corr(int bmtIdx, int64_t x) const {
    return _bmts[bmtIdx]._a ^ x;
}

AbstractSecureExecutor *BitwiseBmtBatchGenerator::reconstruct(int clientRank) {
    throw std::runtime_error("Not support.");
}

int BitwiseBmtBatchGenerator::msgTagCount() {
    return 2 * RandOtBatchExecutor::msgTagCount();
}
