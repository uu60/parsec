
#include "intermediate/BmtGenerator.h"

#include "utils/Math.h"
#include "comm/Comm.h"
#include "conf/Conf.h"
#include "ot/IknpOtBatchOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"

void BmtGenerator::generateRandomAB() {
    _bmt._a = ring(Math::randInt());
    _bmt._b = ring(Math::randInt());
}

int64_t BmtGenerator::corr(int i, int64_t x) const {
    return ring((_bmt._a << i) - x);
}

BmtGenerator *BmtGenerator::reconstruct(int clientRank) {
    throw std::runtime_error("Not support.");
}

int BmtGenerator::tagStride(int width) {
    return 2 * IknpOtBatchOperator::tagStride();
}

void BmtGenerator::computeMix(int sender) {
    int64_t sum = 0;
    bool isSender = Comm::rank() == sender;

    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    if (isSender) {
        ss0.reserve(_width);
        ss1.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            ss0.push_back(Math::randInt());
            ss1.push_back(corr(i, ss0[i]));
        }
    } else {
        choices.reserve(_width);
        for (int i = 0; i < _width; ++i) {
            choices.push_back(static_cast<int>((_bmt._b >> i) & 1));
        }
    }

    auto results = IknpOtBatchOperator(sender, &ss0, &ss1, &choices, _width, _taskTag,
                                       _currentMsgTag + sender * IknpOtBatchOperator::tagStride()).execute()->
            _results;

    if (isSender) {
        for (int i = 0; i < ss0.size(); ++i) {
            sum += ss0[i];
        }
    } else {
        for (int i = 0; i < choices.size(); ++i) {
            int64_t temp = results[i];
            if (choices[i] == 0) {
                temp = -temp;
            }
            sum += temp;
        }
    }

    if (sender == 0) {
        _ui = ring(sum);
    } else {
        _vi = ring(sum);
    }
}

void BmtGenerator::computeC() {
    _bmt._c = ring(_bmt._a * _bmt._b + _ui + _vi);
}

BmtGenerator *BmtGenerator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
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

    return this;
}
