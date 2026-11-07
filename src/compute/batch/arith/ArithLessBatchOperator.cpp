
#include "parsecdb/compute/batch/arith/ArithLessBatchOperator.h"

#include "parsecdb/comm/Comm.h"
#include "parsecdb/compute/batch/arith/ArithToBoolBatchOperator.h"
#include "parsecdb/conf/Conf.h"
#include "parsecdb/intermediate/BitwiseBmtBatchGenerator.h"
#include "parsecdb/intermediate/IntermediateDataSupport.h"
#include "parsecdb/utils/Log.h"
#include "parsecdb/utils/Math.h"
#include "parsecdb/utils/System.h"

namespace parsecdb {

ArithLessBatchOperator::ArithLessBatchOperator(std::vector<int64_t>* xs, std::vector<int64_t>* ys, int width,
                                               int taskTag, int msgTagOffset, int clientRank)
    : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {}

ArithLessBatchOperator* ArithLessBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    if (Conf::BMT_METHOD != Conf::BMT_JIT && Conf::BMT_METHOD != Conf::BMT_FIXED) {
        throw std::runtime_error(
            "ArithLessBatchOperator: Temporarily only support BMT JIT or FIXED generation for experiment.");
    }

    int64_t start = 0;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }

    int num = static_cast<int>(_xis->size());

    std::vector<int64_t> a_deltas(num);
    for (int i = 0; i < num; i++) {
        a_deltas[i] = Math::ringSubtract((*_xis)[i], (*_yis)[i], _width);
    }

    ArithToBoolBatchOperator toBoolOp(&a_deltas, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    toBoolOp.execute();

    _zis.resize(num);
    for (int i = 0; i < num; i++) {
        _zis[i] = Math::getBit(toBoolOp._zis[i], _width - 1);
    }

    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }

    return this;
}

ArithLessBatchOperator* ArithLessBatchOperator::reconstruct(int clientRank) {
    ArithBatchOperator::reconstruct(clientRank);

    if (Comm::rank() == clientRank) {
        for (int i = 0; i < _results.size(); i++) {
            _results[i] &= 1;
        }
    }

    return this;
}

int ArithLessBatchOperator::tagStride(int width) { return ArithToBoolBatchOperator::tagStride(width); }

} // namespace parsecdb
