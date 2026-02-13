#include "../../../../include/compute/batch/bool/BoolEqualBatchOperator.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolXorBatchOperator.h"

BoolEqualBatchOperator *BoolEqualBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    std::vector<BitwiseBmt> allBmts;
    bool gotBmt = prepareBmts(allBmts);

    const int lessBmtCount = BoolLessBatchOperator::bmtCount(_xis->size(), _width);
    std::vector<BitwiseBmt> bmtsLessXy;
    std::vector<BitwiseBmt> bmtsLessYx;
    if (gotBmt) {
        bmtsLessYx = std::vector(allBmts.end() - lessBmtCount, allBmts.end());
        allBmts.resize(allBmts.size() - lessBmtCount);
        bmtsLessXy = std::vector(allBmts.end() - lessBmtCount, allBmts.end());
        allBmts.resize(allBmts.size() - lessBmtCount);
    }

    auto gtv = BoolLessBatchOperator(_xis, _yis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &bmtsLessXy : nullptr)->execute()->_zis;
    auto ltv = BoolLessBatchOperator(_yis, _xis, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &bmtsLessYx : nullptr)->execute()->_zis;

    for (auto &v: gtv) {
        v = v ^ Comm::rank();
    }
    for (auto &v: ltv) {
        v = v ^ Comm::rank();
    }

    _zis = BoolAndBatchOperator(&gtv, &ltv, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &allBmts : nullptr)->execute()->_zis;
    return this;
}

BoolEqualBatchOperator *BoolEqualBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    this->_bmts = bmts;
    return this;
}

int BoolEqualBatchOperator::tagStride() {
    return std::max(BoolLessBatchOperator::tagStride(), BoolAndBatchOperator::tagStride());
}

int BoolEqualBatchOperator::bmtCount(int num, int width) {
    return 2 * BoolLessBatchOperator::bmtCount(num, width) + BoolAndBatchOperator::bmtCount(num, width);
}

bool BoolEqualBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}
