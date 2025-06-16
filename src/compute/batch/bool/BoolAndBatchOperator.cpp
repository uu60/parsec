//
// Created by 杜建璋 on 2025/2/24.
//

#include "compute/batch/bool/BoolAndBatchOperator.h"

#include "accelerate/SimdSupport.h"
#include "conf/Conf.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"

int BoolAndBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return bmts.size();
    }
    int num = static_cast<int>(_xis->size() * (_doWithConditions ? 2 : 1));
    int totalBits = num * _width;
    int bc = -1; // -1 means required bmt bits less than 64
    if (totalBits > 64) {
        // ceil division
        bc = (totalBits + 63) / 64;
    }
    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
        if (bc == -1) {
            bmts = IntermediateDataSupport::pollBitwiseBmts(1, totalBits);
        } else {
            bmts = IntermediateDataSupport::pollBitwiseBmts(bc, 64);
        }
    } else if (Conf::BMT_METHOD == Conf::BMT_JIT) {
        if (bc == -1) {
            bmts = BitwiseBmtBatchGenerator(1, totalBits, _taskTag, _currentMsgTag).execute()->_bmts;
        } else {
            bmts = BitwiseBmtBatchGenerator(bc, 64, _taskTag, _currentMsgTag).execute()->_bmts;
        }
    }
    return bc;
}

BoolAndBatchOperator::BoolAndBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                           std::vector<int64_t> *conds, int width, int taskTag,
                                           int msgTagOffset) : BoolBatchOperator(
    xs, ys, width, taskTag, msgTagOffset, NO_CLIENT_COMPUTE) {
    _conds_i = conds;
    _doWithConditions = true;
}

BoolAndBatchOperator *BoolAndBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_doWithConditions) {
        executeForMutex();
    } else {
        execute0();
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int BoolAndBatchOperator::tagStride() {
    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        return 1;
    }
    return BitwiseBmtBatchGenerator::tagStride();
}

BoolAndBatchOperator *BoolAndBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    _bmts = bmts;
    return this;
}

int BoolAndBatchOperator::bmtCount(int num, int width) {
    return (num * width + 63) / 64;
}

void BoolAndBatchOperator::execute0() {
    std::vector<BitwiseBmt> bmts;
    int bc = prepareBmts(bmts);
    int num = static_cast<int>(_xis->size());

    // The first num elements are ei, and the left num elements are fi
    std::vector<int64_t> efi(num * 2);


    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        for (int i = 0; i < num; i++) {
            efi[i] = (*_xis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._a;
            efi[num + i] = (*_yis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._b;
        }
    } else {
        if (_width < 64) {
            for (int i = 0; i < num; i++) {
                auto bmt = BitwiseBmt::extract(bmts, i, _width);
                efi[i] = (*_xis)[i] ^ bmt._a;
                efi[num + i] = (*_yis)[i] ^ bmt._b;
            }
        } else {
            for (int i = 0; i < num; i++) {
                efi[i] = (*_xis)[i] ^ bmts[i]._a;
                efi[num + i] = (*_yis)[i] ^ bmts[i]._b;
            }
        }
    }

    std::vector<int64_t> efo;
    auto r0 = Comm::serverSendAsync(efi, _width, buildTag(_currentMsgTag));
    auto r1 = Comm::serverReceiveAsync(efo, efi.size(), _width, buildTag(_currentMsgTag));
    Comm::wait(r0);
    Comm::wait(r1);

    std::vector<int64_t> efs;

    // Verified SIMD performance
    if (Conf::ENABLE_SIMD) {
        efs = SimdSupport::xorV(efi, efo);
    } else {
        efs = std::vector<int64_t>(num * 2);
        for (int i = 0; i < num * 2; i++) {
            efs[i] = efi[i] ^ efo[i];
        }
    }

    _zis.resize(num);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        for (int i = 0; i < num; i++) {
            int64_t e = efs[i];
            int64_t f = efs[num + i];
            _zis[i] = Math::ring((extendedRank & e & f) ^ (
                                     f & IntermediateDataSupport::_fixedBitwiseBmt._a) ^ (
                                     e & IntermediateDataSupport::_fixedBitwiseBmt._b) ^
                                 IntermediateDataSupport::_fixedBitwiseBmt._c, _width);
        }
    } else {
        if (_width < 64 && bc != -2) {
            for (int i = 0; i < num; i++) {
                int64_t e = efs[i];
                int64_t f = efs[num + i];
                auto bmt = BitwiseBmt::extract(bmts, i, _width);

                _zis[i] = Math::ring((extendedRank & e & f) ^ (f & bmt._a) ^ (e & bmt._b) ^ bmt._c, _width);
            }
        } else {
            for (int i = 0; i < num; i++) {
                int64_t e = efs[i];
                int64_t f = efs[num + i];
                auto bmt = bmts[i];
                int64_t a = bmt._a;
                int64_t b = bmt._b;
                int64_t c = bmt._c;
                _zis[i] = Math::ring((extendedRank & e & f) ^ (f & a) ^ (e & b) ^ c, _width);
            }
        }
    }
}

void BoolAndBatchOperator::executeForMutex() {
    std::vector<BitwiseBmt> bmts;
    auto num = _xis->size();
    auto condNum = _conds_i->size();
    int bc;

    // The first num * 2 elements are ei (xi and yi), and the other num * 2 elements are fi (condi and condi)
    std::vector<int64_t> efi(num * 4);

    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        for (int i = 0; i < num; i++) {
            efi[i] = (*_xis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._a;
            efi[num + i] = (*_yis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._a;

            int64_t fi = (*_conds_i)[i % condNum] ^ IntermediateDataSupport::_fixedBitwiseBmt._b;
            efi[2 * num + i] = fi;
            efi[3 * num + i] = fi;
        }
    } else {
        bc = prepareBmts(bmts);
        if (_width < 64 && bc != -2) {
            for (int i = 0; i < num; i++) {
                // Multiple 64 bit bmts
                auto bmt = BitwiseBmt::extract(bmts, i, _width);
                efi[i] = (*_xis)[i] ^ bmt._a;
                efi[num * 2 + i] = (*_conds_i)[i % condNum] ^ bmt._b;
            }
            for (int i = num; i < num * 2; i++) {
                // Multiple 64 bit bmts
                auto bmt = BitwiseBmt::extract(bmts, i, _width);
                efi[i] = (*_yis)[i - num] ^ bmt._a;
                efi[num * 2 + i] = (*_conds_i)[i % condNum] ^ bmt._b;
            }
        } else {
            for (int i = 0; i < num; i++) {
                efi[i] = (*_xis)[i] ^ bmts[i]._a;
                efi[num * 2 + i] = (*_conds_i)[i % condNum] ^ bmts[i]._b;
            }
            for (int i = num; i < num * 2; i++) {
                efi[i] = (*_yis)[i - num] ^ bmts[i]._a;
                efi[num * 2 + i] = (*_conds_i)[i % condNum] ^ bmts[i]._b;
            }
        }
    }

    std::vector<int64_t> efo;
    auto r0 = Comm::serverSendAsync(efi, _width, buildTag(_currentMsgTag));
    auto r1 = Comm::serverReceiveAsync(efo, efi.size(), _width, buildTag(_currentMsgTag));
    Comm::wait(r0);
    Comm::wait(r1);

    std::vector<int64_t> efs;

    // Verified SIMD performance
    if (Conf::ENABLE_SIMD) {
        efs = SimdSupport::xorV(efi, efo);
    } else {
        efs = std::vector<int64_t>(num * 4);
        for (int i = 0; i < num * 4; i++) {
            efs[i] = efi[i] ^ efo[i];
        }
    }

    _zis.resize(num * 2);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        for (int i = 0; i < num * 2; i++) {
            int64_t e = efs[i];
            int64_t f = efs[num * 2 + i];
            _zis[i] = Math::ring((extendedRank & e & f) ^ (
                                     f & IntermediateDataSupport::_fixedBitwiseBmt._a) ^ (
                                     e & IntermediateDataSupport::_fixedBitwiseBmt._b) ^
                                 IntermediateDataSupport::_fixedBitwiseBmt._c, _width);
        }
    } else {
        if (_width < 64 && bc != -2) {
            for (int i = 0; i < num * 2; i++) {
                int64_t e = efs[i];
                int64_t f = efs[num * 2 + i];
                // Multiple 64 bit bmts
                auto bmt = BitwiseBmt::extract(bmts, i, _width);
                _zis[i] = Math::ring((extendedRank & e & f) ^ (f & bmt._a) ^ (e & bmt._b) ^ bmt._c, _width);
            }
        } else {
            for (int i = 0; i < num * 2; i++) {
                int64_t e = efs[i];
                int64_t f = efs[num * 2 + i];
                // Multiple 64 bit bmts
                _zis[i] = Math::ring((extendedRank & e & f) ^ (f & bmts[i]._a) ^ (e & bmts[i]._b) ^ bmts[i]._c, _width);
            }
        }
    }
}
