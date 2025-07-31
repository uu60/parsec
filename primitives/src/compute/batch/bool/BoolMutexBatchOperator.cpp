//
// Created by 杜建璋 on 2025/3/1.
//

#include "compute/batch/bool/BoolMutexBatchOperator.h"

#include "accelerate/SimdSupport.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/single/bool/BoolAndOperator.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

BoolMutexBatchOperator::BoolMutexBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                               std::vector<int64_t> *conds, int width, int taskTag,
                                               int msgTagOffset, int clientRank) : BoolBatchOperator(
    xs, ys, width, taskTag, msgTagOffset, clientRank) {
    if (clientRank == NO_CLIENT_COMPUTE) {
        if (Comm::isClient()) {
            return;
        }
        _conds_i = new std::vector(*conds);
    } else {
        _conds_i = new std::vector(std::move(BoolBatchOperator(*conds, 1, _taskTag, _currentMsgTag, clientRank)._zis));
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

BoolMutexBatchOperator::BoolMutexBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                               std::vector<int64_t> *conds, int width, int taskTag,
                                               int msgTagOffset) : BoolBatchOperator(
    xs, ys, width, taskTag, msgTagOffset, NO_CLIENT_COMPUTE) {
    _conds_i = new std::vector(*conds);
    for (int64_t &ci: *_conds_i) {
        if (ci != 0) {
            // Set to all 1 on each bit
            ci = ring(-1ll);
        }
    }
    _bidir = true;
}

BoolMutexBatchOperator::~BoolMutexBatchOperator() {
    delete _conds_i;
}

bool BoolMutexBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    bool gotBmt = false;
    if (_bmts != nullptr) {
        gotBmt = true;
        bmts = std::move(*_bmts);
    }
    return gotBmt;
}

void BoolMutexBatchOperator::execute0() {
    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    auto num = _conds_i->size();

    auto zis = BoolAndBatchOperator(_xis, _yis, _conds_i, _width, _taskTag, _currentMsgTag).setBmts(
        gotBmt ? &bmts : nullptr)->execute()->_zis;

    // Verified SIMD performance
    if (Conf::ENABLE_SIMD) {
        _zis = SimdSupport::xor3(zis.data(), _yis->data(), zis.data() + num, num);
        for (auto &zi : _zis) {
            zi = ring(zi);
        }
    } else {
        _zis.resize(num);
        for (int i = 0; i < num; i++) {
            _zis[i] = ring(zis[i] ^ (*_yis)[i] ^ zis[num + i]);
        }
    }
}

void BoolMutexBatchOperator::executeBidirectionally() {
    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    auto num = _xis->size();

    // First half is xis & conds_i, the other is yis & conds_i
    auto zis = BoolAndBatchOperator(_xis, _yis, _conds_i, _width, _taskTag, _currentMsgTag).setBmts(
        gotBmt ? &bmts : nullptr)->execute()->_zis;

    // Verified SIMD performance
    if (Conf::ENABLE_SIMD) {
        _zis = SimdSupport::xor3Concat(zis.data(), _yis->data(), _xis->data(), zis.data() + num, num);
        for (auto &zi : _zis) {
            zi = ring(zi);
        }
    } else {
        _zis.resize(num * 2);
        for (int i = 0; i < num; i++) {
            _zis[i] = ring(zis[i] ^ (*_yis)[i] ^ zis[num + i]);
            _zis[num + i] = ring(zis[i] ^ (*_xis)[i] ^ zis[num + i]);
        }
    }
}

BoolMutexBatchOperator *BoolMutexBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_bidir) {
        executeBidirectionally();
    } else {
        execute0();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolMutexBatchOperator *BoolMutexBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    _bmts = bmts;
    return this;
}

int BoolMutexBatchOperator::tagStride() {
    return BoolAndBatchOperator::tagStride();
}

int BoolMutexBatchOperator::bmtCount(int num, int width) {
    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        return 0;
    }
    return 2 * BoolAndBatchOperator::bmtCount(num, width);
}
