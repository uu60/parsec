
#include "parsec/compute/batch/arith/ArithMutexBatchOperator.h"

#include "parsec/comm/Comm.h"
#include "parsec/compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "parsec/compute/batch/bool/BoolToArithBatchOperator.h"
#include "parsec/compute/single/bool/BoolToArithOperator.h"
#include "parsec/conf/Conf.h"
#include "parsec/intermediate/BmtBatchGenerator.h"
#include "parsec/intermediate/IntermediateDataSupport.h"
#include "parsec/parallel/ThreadPoolSupport.h"
#include "parsec/utils/Log.h"
#include "parsec/utils/Math.h"

namespace parsec {

ArithMutexBatchOperator::ArithMutexBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys,
                                                 std::vector<int64_t>* conds, int width, int taskTag, int msgTagOffset,
                                                 int clientRank)
    : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
    if (clientRank < 0 && _width > 1) {
        _conds_i = BoolToArithBatchOperator(conds, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).execute()->_zis;
    } else {
        _conds_i = ArithBatchOperator(*conds, _width, _taskTag, _currentMsgTag, clientRank)._zis;
    }
}

ArithMutexBatchOperator* ArithMutexBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    int num = static_cast<int>(_xis->size());
    _zis.resize(num);

    std::vector<int64_t> cx_results, cy_results;
    std::future<std::vector<int64_t>> f;

    if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
        f = ThreadPoolSupport::submit([&]() -> std::vector<int64_t> {
            auto mul_cx =
                ArithMultiplyBatchOperator(&_conds_i, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
            return mul_cx.execute()->_zis;
        });
    } else {
        auto mul_cx = ArithMultiplyBatchOperator(&_conds_i, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
        cx_results = mul_cx.execute()->_zis;
    }

    auto mul_cy =
        ArithMultiplyBatchOperator(&_conds_i, _yis, _width, _taskTag,
                                   _currentMsgTag + ArithMultiplyBatchOperator::tagStride(_width), NO_CLIENT_COMPUTE);
    cy_results = mul_cy.execute()->_zis;

    if (Conf::ENABLE_INTRA_OPERATOR_PARALLELISM) {
        cx_results = f.get();
    }

    for (int i = 0; i < num; i++) {
        _zis[i] = Math::ringSubtract(Math::ringAdd(cx_results[i], (*_yis)[i], _width), cy_results[i], _width);
    }

    return this;
}

ArithMutexBatchOperator* ArithMutexBatchOperator::reconstruct(int clientRank) {
    ArithBatchOperator::reconstruct(clientRank);
    return this;
}

int ArithMutexBatchOperator::tagStride(int width) {
    return std::max(static_cast<int>(2 * ArithMultiplyBatchOperator::tagStride(width)),
                    BoolToArithBatchOperator::tagStride());
}

int ArithMutexBatchOperator::bmtCount(int width, int batchSize) { return 2 * batchSize; }

ArithMutexBatchOperator* ArithMutexBatchOperator::setBmts(std::vector<Bmt>* bmts) {
    if (bmts != nullptr && bmts->size() != bmtCount(_width, static_cast<int>(_xis->size()))) {
        throw std::runtime_error("Mismatch bmt size.");
    }
    _bmts = bmts;
    return this;
}

} // namespace parsec
