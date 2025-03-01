//
// Created by 杜建璋 on 2024/12/29.
//

#include <cmath>

#include "compute/single/bool/BoolLessExecutor.h"
#include "compute/single/bool/BoolAndExecutor.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

void BoolLessExecutor::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = *_bmts;
    } else {
        int bc = bmtCount(_width);
        if (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
            bmts = IntermediateDataSupport::pollBitwiseBmts(bc, _width);
        } else if (Conf::BMT_METHOD == Consts::BMT_JIT) { // JIT BMT
            if (!Conf::TASK_BATCHING) {
                bmts = BitwiseBmtBatchGenerator(bc, _width, _taskTag, _currentMsgTag).execute()->_bmts;
            } else if (Conf::INTRA_OPERATOR_PARALLELISM) {
                std::vector<std::future<BitwiseBmt> > futures;
                futures.reserve(bc);
                for (int i = 0; i < bc; i++) {
                    futures.push_back(ThreadPoolSupport::submit([&, i] {
                        return BitwiseBmtGenerator(_width, _taskTag,
                                                   static_cast<int16_t>(
                                                       _currentMsgTag + i *
                                                       BitwiseBmtGenerator::msgTagCount(_width))).
                                execute()->_bmt;
                    }));
                }
                bmts.reserve(bc);
                for (auto &f: futures) {
                    bmts.push_back(f.get());
                }
            } else {
                bmts.reserve(bc);
                for (int i = 0; i < bc; i++) {
                    bmts.push_back(BitwiseBmtGenerator(_width, _taskTag, _currentMsgTag).execute()->_bmt);
                }
            }
        } else if (Conf::BMT_METHOD == Consts::BMT_FIXED) {
            bmts.reserve(bc);
            for (int i = 0; i < bc; i++) {
                bmts.push_back(IntermediateDataSupport::_fixedBitwiseBmt);
            }
        }
    }
}

BoolLessExecutor *BoolLessExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }
    std::vector<BitwiseBmt> bmts;
    prepareBmts(bmts);

    int bmtI = 0;
    int64_t x_xor_y = _xi ^ _yi;
    int64_t lbs = Comm::rank() == 0 ? x_xor_y : x_xor_y ^ Math::ring(-1ll, _width);

    int64_t shifted_1 = shiftGreater(lbs, 1);

    lbs = BoolAndExecutor(lbs, shifted_1, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmt(&bmts[bmtI++])->execute()->_zi;

    int64_t diag = Math::changeBit(x_xor_y, 0, Math::getBit(_yi, 0) ^ Comm::rank());
    // diag & x

    diag = BoolAndExecutor(diag, _xi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmt(
        &bmts[bmtI++])->execute()->_zi;

    int rounds = static_cast<int>(std::floor(std::log2(_width)));
    for (int r = 2; r <= rounds; r++) {
        int64_t shifted_r = shiftGreater(lbs, r);

        int64_t and_r = BoolAndExecutor(lbs, shifted_r, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
                setBmt(&bmts[bmtI++])->execute()->_zi;
        lbs = and_r;
    }

    int64_t shifted_accum = Math::changeBit(lbs >> 1, _width - 1, Comm::rank());

    int64_t final_accum = BoolAndExecutor(shifted_accum, diag, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).
            setBmt(&bmts[bmtI++])->execute()->_zi;

    bool result = false;
    for (int i = 0; i < _width; i++) {
        result = result ^ Math::getBit(final_accum, i);
    }

    _zi = result;

    if (Conf::CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

BoolLessExecutor *BoolLessExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    if (bmts->size() != bmtCount(_width)) {
        throw std::runtime_error("Bmt size mismatch.");
    }
    _bmts = bmts;
    return this;
}

int16_t BoolLessExecutor::msgTagCount(int width) {
    return static_cast<int16_t>(bmtCount(width) * BoolAndExecutor::msgTagCount(width));
}

int BoolLessExecutor::bmtCount(int width) {
    return static_cast<int>(std::floor(std::log2(width))) + 2;
}

int64_t BoolLessExecutor::shiftGreater(int64_t in, int r) const {
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

        for (int j = start - 1; j >= i; j--) {
            in = Math::changeBit(in, j, midBit);
        }
    }

    return in;
}
