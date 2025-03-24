//
// Created by 杜建璋 on 2025/2/24.
//

#include "compute/batch/bool/BoolAndBatchExecutor.h"

#include "accelerate/SimdSupport.h"
#include "conf/Conf.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/IntermediateDataSupport.h"

int BoolAndBatchExecutor::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    int num = static_cast<int>(_xis->size() * (_doMutex ? 2 : 1));
    int totalBits = num * _width;
    int bc = -1;
    if (totalBits > 64) {
        // ceil division
        bc = (num * _width + 63) / 64;
    }
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
    } else if constexpr (Conf::BMT_METHOD == Consts::BMT_BACKGROUND) {
        if (bc == -1) {
            bmts = IntermediateDataSupport::pollBitwiseBmts(1, totalBits);
        } else {
            bmts = IntermediateDataSupport::pollBitwiseBmts(bc, 64);
        }
    } else if constexpr (Conf::BMT_METHOD == Consts::BMT_JIT) {
        if (bc == -1) {
            bmts = {BitwiseBmtGenerator(totalBits, _taskTag, _currentMsgTag).execute()->_bmt};
        } else {
            bmts = BitwiseBmtBatchGenerator(bc, 64, _taskTag, _currentMsgTag).execute()->_bmts;
        }
    }
    return bc;
}

BoolAndBatchExecutor::BoolAndBatchExecutor(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                           std::vector<int64_t> *conds, int width, int taskTag,
                                           int msgTagOffset) : BoolBatchExecutor(
    xs, ys, width, taskTag, msgTagOffset, NO_CLIENT_COMPUTE) {
    _conds_i = conds;
    _doMutex = true;
}

BoolAndBatchExecutor *BoolAndBatchExecutor::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int64_t start;
    if constexpr (Conf::CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    if (_doMutex) {
        executeMutex();
    } else {
        execute0();
    }

    if constexpr (Conf::CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

int BoolAndBatchExecutor::msgTagCount(int num, int width) {
    if constexpr (Conf::BMT_METHOD == Consts::BMT_FIXED) {
        return 1;
    }
    return BitwiseBmtBatchGenerator::msgTagCount(bmtCount(num), width);
}

BoolAndBatchExecutor *BoolAndBatchExecutor::setBmts(std::vector<BitwiseBmt> *bmts) {
    _bmts = bmts;
    return this;
}

int BoolAndBatchExecutor::bmtCount(int num) {
    return num;
}

void BoolAndBatchExecutor::execute0() {
    std::vector<BitwiseBmt> bmts;
    int bc = prepareBmts(bmts);
    int num = static_cast<int>(_xis->size());

    // The first num elements are ei, and the left num elements are fi
    std::vector<int64_t> efi(num * 2);

    for (int i = 0; i < num; i++) {
        if constexpr (Conf::BMT_METHOD == Consts::BMT_FIXED) {
            efi[i] = (*_xis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._a;
            efi[num + i] = (*_yis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._b;
        } else {
            if (_width < 64) {
                // Multiple 64 bit bmts
                int64_t mask = (1ll << _width) - 1;
                auto &bmt = bmts[i * _width / 64];
                int offset = i % 64 * _width;
                efi[i] = (*_xis)[i] ^ (((bmt._a) & (mask << offset)) >> offset);
                efi[num + i] = (*_yis)[i] ^ (((bmt._b) & (mask << offset)) >> offset);
            } else {
                efi[i] = (*_xis)[i] ^ bmts[i]._a;
                efi[num + i] = (*_yis)[i] ^ bmts[i]._b;
            }
        }
    }

    std::vector<int64_t> efo;
    auto r = Comm::serverSendAsync(efi, _width, buildTag(_currentMsgTag));
    Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
    Comm::wait(r);

    std::vector<int64_t> efs;

    // Verified SIMD performance
    if constexpr (Conf::ENABLE_SIMD) {
        efs = SimdSupport::xorV(efi, efo);
    } else {
        efs = std::vector<int64_t>(num * 2);
        for (int i = 0; i < num; i++) {
            efs[i] = efi[i] ^ efo[i];
            efs[num + i] = efi[num + i] ^ efo[num + i];
        }
    }

    _zis.resize(num);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    for (int i = 0; i < num; i++) {
        int64_t e = efs[i];
        int64_t f = efs[num + i];
        if constexpr (Conf::BMT_METHOD == Consts::BMT_FIXED) {
            _zis[i] = (extendedRank & e & f) ^ (
                          f & IntermediateDataSupport::_fixedBitwiseBmt._a) ^ (
                          e & IntermediateDataSupport::_fixedBitwiseBmt._b) ^
                      IntermediateDataSupport::_fixedBitwiseBmt._c;
        } else {
            // Multiple 64 bit bmts
            int64_t a, b, c;
            if (_width < 64) {
                int64_t mask = (1ll << _width) - 1;
                auto &bmt = bmts[i * _width / 64];
                int offset = i % 64 * _width;
                a = (bmt._a & (mask << offset)) >> offset;
                b = (bmt._b & (mask << offset)) >> offset;
                c = (bmt._c & (mask << offset)) >> offset;
            } else {
                a = bmts[i]._a;
                b = bmts[i]._b;
                c = bmts[i]._c;
            }
            _zis[i] = (extendedRank & e & f) ^ (f & a) ^ (e & b) ^ c;
        }
    }
}

void BoolAndBatchExecutor::executeMutex() {
    std::vector<BitwiseBmt> bmts;
    int bc = prepareBmts(bmts);
    int num = static_cast<int>(_xis->size());

    // The first num * 2 elements are ei (xi and yi), and the other num * 2 elements are fi (condi and condi)
    std::vector<int64_t> efi(num * 4);

    for (int i = 0; i < num; i++) {
        if constexpr (Conf::BMT_METHOD == Consts::BMT_FIXED) {
            efi[i] = (*_xis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._a;
            efi[num + i] = (*_yis)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._a;

            int64_t fi = (*_conds_i)[i] ^ IntermediateDataSupport::_fixedBitwiseBmt._b;
            efi[2 * num + i] = fi;
            efi[3 * num + i] = fi;
        } else {
            // TODO
        }
    }

    std::vector<int64_t> efo;
    auto r = Comm::serverSendAsync(efi, _width, buildTag(_currentMsgTag));
    Comm::serverReceive(efo, _width, buildTag(_currentMsgTag));
    Comm::wait(r);

    std::vector<int64_t> efs;

    // Verified SIMD performance
    if constexpr (Conf::ENABLE_SIMD) {
        efs = SimdSupport::xorV(efi, efo);
    } else {
        efs = std::vector<int64_t>(num * 2);
        for (int i = 0; i < num; i++) {
            efs[i] = efi[i] ^ efo[i];
            efs[num + i] = efi[num + i] ^ efo[num + i];
        }
    }

    _zis.resize(num * 2);
    int64_t extendedRank = Comm::rank() ? ring(-1ll) : 0;

    for (int i = 0; i < num * 2; i++) {
        int64_t e = efs[i];
        int64_t f = efs[num * 2 + i];
        if constexpr (Conf::BMT_METHOD == Consts::BMT_FIXED) {
            _zis[i] = (extendedRank & e & f) ^ (
                          f & IntermediateDataSupport::_fixedBitwiseBmt._a) ^ (
                          e & IntermediateDataSupport::_fixedBitwiseBmt._b) ^
                      IntermediateDataSupport::_fixedBitwiseBmt._c;
        } else {
            // Multiple 64 bit bmts
            int64_t a, b, c;
            if (_width < 64) {
                int64_t mask = (1ll << _width) - 1;
                auto &bmt = bmts[i * _width / 64];
                int offset = i % 64 * _width;
                a = (bmt._a & (mask << offset)) >> offset;
                b = (bmt._b & (mask << offset)) >> offset;
                c = (bmt._c & (mask << offset)) >> offset;
            } else {
                a = bmts[i]._a;
                b = bmts[i]._b;
                c = bmts[i]._c;
            }
            _zis[i] = (extendedRank & e & f) ^ (f & a) ^ (e & b) ^ c;
        }
    }
}
