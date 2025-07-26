//
// Created by 杜建璋 on 2025/7/23.
//

#include "compute/batch/arith/ArithMutexBatchOperator.h"

#include "comm/Comm.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "compute/batch/bool/BoolToArithBatchOperator.h"
#include "compute/single/bool/BoolToArithOperator.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "intermediate/BmtBatchGenerator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithMutexBatchOperator::ArithMutexBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, 
                                                 std::vector<int64_t> *conds, int width, int taskTag, 
                                                 int msgTagOffset, int clientRank)
    : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    
    // if (clientRank < 0 && conds != nullptr) {
    //     // Convert boolean conditions to arithmetic shares if needed using batch operation
    //     if (_width > 1) {
    //         // Use batch conversion for better performance
    //         _conds_i = BoolToArithBatchOperator(conds, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zis;
    //     } else {
    //         _conds_i = *conds;
    //     }
    // } else if (conds != nullptr) {
    //     _conds_i = *conds;
    // }

    if (clientRank < 0 && _width > 1) {
        _conds_i = BoolToArithBatchOperator(conds, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        _conds_i = ArithBatchOperator(*conds, _width, _taskTag, _currentMsgTag, clientRank)._zis;
    }
}

ArithMutexBatchOperator *ArithMutexBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int num = static_cast<int>(_xis->size());
    _zis.resize(num);  // Result contains swapped x values

    std::vector<int64_t> cx_results, cy_results;
    std::future<std::vector<int64_t>> f;

    if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
        // Parallel execution: compute cond * x and cond * y in parallel
        f = ThreadPoolSupport::submit([&]() -> std::vector<int64_t> {
            auto mul_cx = ArithMultiplyBatchOperator(&_conds_i, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            return mul_cx.execute()->_zis;
        });
    } else {
        // Sequential execution: compute cond * x first
        auto mul_cx = ArithMultiplyBatchOperator(&_conds_i, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
        cx_results = mul_cx.execute()->_zis;
    }

    // Compute cond * y
    auto mul_cy = ArithMultiplyBatchOperator(&_conds_i, _yis, _width, _taskTag,
                                           _currentMsgTag + ArithMultiplyBatchOperator::tagStride(_width), 
                                           NO_CLIENT_COMPUTE);
    cy_results = mul_cy.execute()->_zis;

    if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
        cx_results = f.get();
    }

    // Compute final result: new_x = cond * x + (1-cond) * y = cx + y - cy
    for (int i = 0; i < num; i++) {
        _zis[i] = ring(cx_results[i] + (*_yis)[i] - cy_results[i]);
    }

    return this;
}

ArithMutexBatchOperator *ArithMutexBatchOperator::reconstruct(int clientRank) {
    ArithBatchOperator::reconstruct(clientRank);
    return this;
}

int ArithMutexBatchOperator::tagStride(int width) {
    return std::max(static_cast<int>(2 * ArithMultiplyBatchOperator::tagStride(width)),
                    BoolToArithBatchOperator::tagStride());
}

int ArithMutexBatchOperator::bmtCount(int width, int batchSize) {
    return 2 * batchSize;  // Two multiplications per element
}

ArithMutexBatchOperator *ArithMutexBatchOperator::setBmts(std::vector<Bmt> *bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width, static_cast<int>(_xis->size()))) {
        throw std::runtime_error("Mismatch bmt size.");
    }
    _bmts = bmts;
    return this;
}
