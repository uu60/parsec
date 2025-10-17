
#include <cmath>

#include "compute/single/bool/BoolLessOperator.h"
#include "compute/single/bool/BoolAndOperator.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

bool BoolLessOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}

BoolLessOperator *BoolLessOperator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }
    std::vector<BitwiseBmt> bmts;
    bool gotBmt = prepareBmts(bmts);

    int bmtI = 0;
    int64_t x_xor_y = _xi ^ _yi;
    int64_t lbs = Comm::rank() == 0 ? x_xor_y : (x_xor_y ^ Math::ring(-1ll, _width));

    int64_t shifted_1 = shiftGreater(lbs, 1);

    lbs = BoolAndOperator(lbs, shifted_1, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmt(gotBmt ? &bmts[bmtI++] : nullptr)->execute()->_zi;

    int64_t diag = Math::changeBit(x_xor_y, 0, Math::getBit(_yi, 0) ^ Comm::rank());

    diag = BoolAndOperator(diag, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(
        gotBmt ? &bmts[bmtI++] : nullptr)->execute()->_zi;

    int rounds = static_cast<int>(std::floor(std::log2(_width)));
    for (int r = 2; r <= rounds; r++) {
        int64_t shifted_r = shiftGreater(lbs, r);

        lbs = BoolAndOperator(lbs, shifted_r, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                setBmt(gotBmt ? &bmts[bmtI++] : nullptr)->execute()->_zi;
    }

    int64_t shifted_accum = Math::changeBit(lbs >> 1, _width - 1, Comm::rank());

    int64_t final_accum = BoolAndOperator(shifted_accum, diag, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            setBmt(gotBmt ? &bmts[bmtI++] : nullptr)->execute()->_zi;

        bool result = false;
    for (int i = 0; i < _width; i++) {
        result = result ^ Math::getBit(final_accum, i);
    }

    _zi = result;

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolLessOperator *BoolLessOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Bmt size mismatch.");
    }
    _bmts = bmts;
    return this;
}

int BoolLessOperator::tagStride(int width) {
    return bmtCount(width) * BitwiseBmtGenerator::tagStride(width);
}

int BoolLessOperator::bmtCount(int width) {
    return static_cast<int>(std::floor(std::log2(width))) + 3;
}

int64_t BoolLessOperator::shiftGreater(int64_t in, int r) const {
    int part_size = 1 << r;
    if (part_size > _width) {
        return in;
    }

    int offset = part_size >> 1;

    for (int i = 0; i < _width; i += part_size) {
        int start = i + offset;
        if (start >= _width) {
            break;
        }

        bool midBit = Math::getBit(in, start);
        int count = start - i;
        int64_t mask = ((1LL << count) - 1) << i;

        if (midBit) {
            in |= mask;
        } else {
            in &= ~mask;
        }
    }

    return in;
}
