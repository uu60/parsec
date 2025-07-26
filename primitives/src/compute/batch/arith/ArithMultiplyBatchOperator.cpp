//
// Created by 杜建璋 on 2025/7/23.
//

#include "compute/batch/arith/ArithMultiplyBatchOperator.h"

#include "comm/Comm.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "intermediate/BmtBatchGenerator.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithMultiplyBatchOperator::ArithMultiplyBatchOperator(std::vector<int64_t> *xs, std::vector<int64_t> *ys,
                                                       int width, int taskTag, int msgTagOffset, int clientRank)
    : ArithBatchOperator(xs, ys, width, taskTag, msgTagOffset, clientRank) {
}

ArithMultiplyBatchOperator *ArithMultiplyBatchOperator::execute() {
    _currentMsgTag = _startMsgTag;

    if (Comm::isClient()) {
        return this;
    }

    if (Conf::BMT_METHOD != Conf::BMT_JIT) {
        throw std::runtime_error("Temporarily only support BMT JIT generation for experiment.");
    }

    int num = static_cast<int>(_xis->size());
    _zis.resize(num);
    
    std::vector<Bmt> bmts = BmtBatchGenerator(num, _width, _taskTag, _currentMsgTag).execute()->_bmts;

    std::vector<int64_t> eis(num), fis(num);
    for (int i = 0; i < num; i++) {
        const Bmt &bmt = bmts[i];
        eis[i] = ring((*_xis)[i] - bmt._a);
        fis[i] = ring((*_yis)[i] - bmt._b);
    }

    std::vector<int64_t> batchEFI;
    batchEFI.reserve(num * 2);
    for (int i = 0; i < num; i++) {
        batchEFI.push_back(eis[i]);
        batchEFI.push_back(fis[i]);
    }

    int batchTag = buildTag(_currentMsgTag);
    auto r0 = Comm::serverSendAsync(batchEFI, _width, batchTag);

    std::vector<int64_t> batchEFO;
    auto r1 = Comm::serverReceiveAsync(batchEFO, num * 2, _width, batchTag);

    Comm::wait(r0);
    Comm::wait(r1);

    for (int i = 0; i < num; i++) {
        int64_t e_sum = ring(eis[i] + batchEFO[2 * i]);
        int64_t f_sum = ring(fis[i] + batchEFO[2 * i + 1]);

        const Bmt &bmt = bmts[i];
        _zis[i] = ring(Comm::rank() * e_sum * f_sum + f_sum * bmt._a + e_sum * bmt._b + bmt._c);
    }

    return this;
}

ArithMultiplyBatchOperator *ArithMultiplyBatchOperator::reconstruct(int clientRank) {
    ArithBatchOperator::reconstruct(clientRank);
    return this;
}

int ArithMultiplyBatchOperator::tagStride(int width) {
    return BmtBatchGenerator::tagStride();
}
