//
// Created by 杜建璋 on 2025/3/1.
//

#include "compute/batch/bool/BoolMutexBatchExecutor.h"

#include "accelerate/SimdSupport.h"
#include "compute/batch/bool/BoolAndBatchExecutor.h"
#include "compute/single/bool/BoolAndExecutor.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

BoolMutexBatchExecutor::BoolMutexBatchExecutor(std::vector<int64_t> &xs, std::vector<int64_t> &ys,
                                               std::vector<int64_t> &conds, int width, int taskTag,
                                               int msgTagOffset, int clientRank) : BoolBatchExecutor(
    xs, ys, width, taskTag, msgTagOffset, clientRank) {
    if (clientRank == NO_CLIENT_COMPUTE) {
        if (Comm::isClient()) {
            return;
        }
        _conds_i = std::move(conds);
    } else {
        _conds_i = BoolBatchExecutor(conds, 1, _taskTag, _currentMsgTag, clientRank)._zis;
    }
    if (Comm::isClient()) {
        return;
    }
    for (int64_t &i: _conds_i) {
        if (i != 0) {
            // Set to all 1 on each bit
            i = ring(-1ll);
        }
    }
}

bool BoolMutexBatchExecutor::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    bool gotBmt = false;
    if (_bmts != nullptr) {
        gotBmt = true;
        bmts = std::move(*_bmts);
    } else if constexpr (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
        gotBmt = true;
        bmts = IntermediateDataSupport::pollBitwiseBmts(_conds_i.size() * 2, _width);
    }
    return gotBmt;
}

BoolMutexBatchExecutor *BoolMutexBatchExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if constexpr (Conf::CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    int num = static_cast<int>(_conds_i.size());

    _conds_i.reserve(num * 2);
    _xis.reserve(num * 2);
    _conds_i.insert(_conds_i.end(), _conds_i.begin(), _conds_i.end());
    // xis now contain both x and y
    _xis.insert(_xis.end(), _yis.begin(), _yis.end());

    auto zis = BoolAndBatchExecutor(_conds_i, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;

    // Verified SIMD performance
    if constexpr (Conf::ENABLE_SIMD) {
        _zis = SimdSupport::xor3(zis.data(), _yis.data(), zis.data() + num, num);
    } else {
        _zis.resize(num);
        for (int i = 0; i < num; i++) {
            _zis[i] = zis[i] ^ _yis[i] ^ zis[num + i];
        }
    }

    if constexpr (Conf::CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolMutexBatchExecutor *BoolMutexBatchExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Mismatched bmts count");
    }
    _bmts = bmts;
    return this;
}

int BoolMutexBatchExecutor::msgTagCount(int num, int width) {
    return BoolAndBatchExecutor::msgTagCount(num, width);
}

int BoolMutexBatchExecutor::bmtCount(int num) {
    if constexpr (Conf::BMT_METHOD == Consts::BMT_FIXED) {
        return 0;
    }
    return num * 2;
}
