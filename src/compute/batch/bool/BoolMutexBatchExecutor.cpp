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

BoolMutexBatchExecutor::BoolMutexBatchExecutor(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                               std::vector<int64_t> *conds, int width, int taskTag,
                                               int msgTagOffset, int clientRank) : BoolBatchExecutor(
    xs, ys, width, taskTag, msgTagOffset, clientRank) {
    if (clientRank == NO_CLIENT_COMPUTE) {
        if (Comm::isClient()) {
            return;
        }
        _conds_i = conds;
    } else {
        _conds_i = new std::vector(std::move(BoolBatchExecutor(*conds, 1, _taskTag, _currentMsgTag, clientRank)._zis));
        _dc = true;
    }
    if (Comm::isClient()) {
        return;
    }
    for (int64_t &ci: *_conds_i) {
        if (ci != 0) {
            // Set to all 1 on each bit
            ci = ring(-1ll);
        }
    }
}

BoolMutexBatchExecutor::BoolMutexBatchExecutor(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                               std::vector<int64_t> *conds, int width, int taskTag,
                                               int msgTagOffset) : BoolBatchExecutor(
    xs, ys, width, taskTag, msgTagOffset, NO_CLIENT_COMPUTE) {
    _conds_i = conds;
    for (int64_t &ci: *_conds_i) {
        if (ci != 0) {
            // Set to all 1 on each bit
            ci = ring(-1ll);
        }
    }
    _doSort = true;
}

BoolMutexBatchExecutor::~BoolMutexBatchExecutor() {
    if (_dc) {
        delete _conds_i;
    }
}

bool BoolMutexBatchExecutor::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    int bc = bmtCount(_xis->size() * (_doSort ? 2 : 1));
    bool gotBmt = false;
    if (_bmts != nullptr) {
        gotBmt = true;
        bmts = std::move(*_bmts);
    } else if constexpr (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
        gotBmt = true;
        bmts = IntermediateDataSupport::pollBitwiseBmts(bc, _width);
    }
    return gotBmt;
}

void BoolMutexBatchExecutor::execute0() {
    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    int num = static_cast<int>(_conds_i->size());

    auto zis = BoolAndBatchExecutor(_xis, _yis, _conds_i, _width, _taskTag, _currentMsgTag).execute()->_zis;

    // Verified SIMD performance
    if constexpr (Conf::ENABLE_SIMD) {
        _zis = SimdSupport::xor3(zis.data(), _yis->data(), zis.data() + num, num);
    } else {
        _zis.resize(num);
        for (int i = 0; i < num; i++) {
            _zis[i] = zis[i] ^ (*_yis)[i] ^ zis[num + i];
        }
    }
}

void BoolMutexBatchExecutor::executeForSort() {
    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    int num = static_cast<int>(_conds_i->size());

    // First half is xis & conds_i, the other is yis & conds_i
    auto zis = BoolAndBatchExecutor(_xis, _yis, _conds_i, _width, _taskTag, _currentMsgTag).execute()->_zis;

    // Verified SIMD performance
    if constexpr (Conf::ENABLE_SIMD) {
        _zis = SimdSupport::xor3Concat(zis.data(), _yis->data(), _xis->data(), zis.data() + num, num);
    } else {
        _zis.resize(num * 2);
        for (int i = 0; i < num; i++) {
            _zis[i] = zis[i] ^ (*_yis)[i] ^ zis[num + i];
            _zis[num + i] = zis[i] ^ (*_xis)[i] ^ zis[num + i];
        }
    }
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

    if (_doSort) {
        executeForSort();
    } else {
        execute0();
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
