//
// Created by 杜建璋 on 2025/3/17.
//

#include "compute/batch/bool/BoolLessBatchOperator.h"

#include "accelerate/SimdSupport.h"
#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/single/bool/BoolLessOperator.h"
#include "conf/Conf.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"

BoolLessBatchOperator::BoolLessBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag,
    int msgTagOffset) : BoolBatchOperator(ys, xs, width, taskTag, msgTagOffset, NO_CLIENT_COMPUTE) {
    _doBidirectional = true;
}

void BoolLessBatchOperator::execute0() {
    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    std::vector<int64_t> x_xor_y, lbs;
    int64_t mask = Math::ring(-1ll, _width);

    x_xor_y.resize(_xis->size());
    for (int i = 0; i < _xis->size(); i++) {
        x_xor_y[i] = (*_xis)[i] ^ (*_yis)[i];
    }
    if (Comm::rank() == 0) {
        lbs = x_xor_y;
    } else {
        lbs.reserve(x_xor_y.size());
        for (int64_t e: x_xor_y) {
            lbs.push_back(e ^ mask);
        }
    }

    auto shifted_1 = shiftGreater(lbs, 1);

    lbs = BoolAndBatchOperator(&lbs, &shifted_1, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;

    std::vector<int64_t> diag;

    // Verified SIMD performance
    if (Conf::ENABLE_SIMD) {
        diag = SimdSupport::computeDiag(*_yis, x_xor_y);
    } else {
        diag.resize(x_xor_y.size());
        for (int i = 0; i < x_xor_y.size(); i++) {
            diag[i] = Math::changeBit(x_xor_y[i], 0, Math::getBit((*_yis)[i], 0) ^ Comm::rank());
        }
    }

    // diag & x
    diag = BoolAndBatchOperator(&diag, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmts(
        gotBmt ? &bmts : nullptr)->execute()->_zis;

    int rounds = static_cast<int>(std::floor(std::log2(_width)));
    for (int r = 2; r <= rounds; r++) {
        auto shifted_r = shiftGreater(lbs, r);

        lbs = BoolAndBatchOperator(&lbs, &shifted_r, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;
    }

    std::vector<int64_t> shifted_accum;
    shifted_accum.reserve(lbs.size());
    for (int i = 0; i < lbs.size(); i++) {
        shifted_accum.push_back(Math::changeBit(lbs[i] >> 1, _width - 1, Comm::rank()));
    }

    auto final_accum = BoolAndBatchOperator(&shifted_accum, &diag, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;

    int fn = static_cast<int>(final_accum.size());
    _zis.resize(fn);
    for (int i = 0; i < fn; i++) {
        bool result = false;
        for (int j = 0; j < _width; j++) {
            result = result ^ Math::getBit(final_accum[i], j);
        }
        _zis[i] = result;
    }
}

void BoolLessBatchOperator::executeBidirectional() {
    // memory copy for xs and ys
    // s0.size() should be equal to s1.size()
    auto s0 = _xis->size();
    auto s1 = _yis->size();

    _xis->reserve(s0 + s1);
    _xis->insert(_xis->end(), _yis->end(), _yis->end());

    _yis->reserve(s0 + s1);
    _yis->insert(_yis->end(), _xis->begin(), _xis->begin() + s0);

    execute0();

    _xis->resize(s0);
    _yis->resize(s1);
}

BoolLessBatchOperator *BoolLessBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_doBidirectional) {
        executeBidirectional();
    } else {
        execute0();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolLessBatchOperator *BoolLessBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    this->_bmts = bmts;
    return this;
}

int BoolLessBatchOperator::msgTagCount() {
    return BoolAndBatchOperator::msgTagCount();
}

int BoolLessBatchOperator::bmtCount(int num, int width) {
    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        return 0;
    }
    return num * BoolLessOperator::bmtCount(width);
}

std::vector<int64_t> BoolLessBatchOperator::shiftGreater(std::vector<int64_t> &in, int r) const {
    int part_size = 1 << r;
    if (part_size > _width) {
        return in;
    }
    int offset = part_size >> 1;

    std::vector<int64_t> out;
    out.reserve(in.size());

    for (int64_t ini: in) {
        for (int i = 0; i < _width; i += part_size) {
            int start = i + offset;
            if (start >= _width) {
                break;
            }

            bool midBit = Math::getBit(ini, start);
            int count = start - i;
            int64_t mask = ((1LL << count) - 1) << i;

            if (midBit) {
                ini |= mask;
            } else {
                ini &= ~mask;
            }
        }
        out.push_back(ini);
    }

    return out;
}

bool BoolLessBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}
