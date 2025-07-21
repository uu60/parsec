//
// Created by 杜建璋 on 25-7-21.
//

#include "compute/batch/arith/ArithLessBatchOperator.h"

#include "comm/Comm.h"
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "conf/Conf.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

ArithLessBatchOperator::ArithLessBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys, int width, int taskTag, int msgTagOffset, int clientRank)
    : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
}

ArithLessBatchOperator *ArithLessBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;
    
    if (Comm::isClient()) {
        return this;
    }
    
    int64_t start;
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        start = System::currentTimeMillis();
    }
    
    int num = static_cast<int>(_xis->size());
    
    // Compute a_delta = xi - yi for all elements
    std::vector<int64_t> a_deltas(num);
    for (int i = 0; i < num; i++) {
        a_deltas[i] = (*_xis)[i] - (*_yis)[i];
    }
    
    // Use ArithToBoolBatchOperator to convert all deltas to boolean representation
    ArithToBoolBatchOperator toBoolOp(&a_deltas, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    toBoolOp.execute();
    
    // Extract the sign bit (MSB) from each boolean result
    _zis.resize(num);
    for (int i = 0; i < num; i++) {
        _zis[i] = (toBoolOp._zis[i] >> (_width - 1)) & 1;
    }
    
    if (Conf::ENABLE_CLASS_WISE_TIMING) {
        _totalTime += System::currentTimeMillis() - start;
    }
    
    return this;
}

ArithLessBatchOperator *ArithLessBatchOperator::reconstruct(int clientRank) {
    ArithBatchOperator::reconstruct(clientRank);
    
    // Ensure results are single bits (0 or 1)
    if (Comm::rank() == clientRank) {
        for (int i = 0; i < _results.size(); i++) {
            _results[i] &= 1;
        }
    }
    
    return this;
}

int ArithLessBatchOperator::tagStride(int width) {
    return ArithToBoolBatchOperator::tagStride(width);
}