
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

    std::vector<BitwiseBmt> bmts;
    if (gotBmt) {
        int bc = BoolLessBatchOperator::bmtCount(_xis->size() * 2, _width);
        bmts = std::vector(allBmts.end() - bc, allBmts.end());
        allBmts.resize(allBmts.size() - bc);
    }

    std::vector<int64_t> xy = *_xis;
    xy.insert(xy.end(), _yis->begin(), _yis->end());
    std::vector<int64_t> yx = *_yis;
    yx.insert(yx.end(), _xis->begin(), _xis->end());

    auto gtv_ltv = BoolLessBatchOperator(&xy, &yx, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE)
            .setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;
    for (auto &v: gtv_ltv) {
        v = v ^ Comm::rank();
    }
    std::vector ltv(gtv_ltv.begin() + gtv_ltv.size() / 2, gtv_ltv.end());
    std::vector gtv = std::move(gtv_ltv);
    gtv.resize(gtv.size() / 2);

    _zis = BoolAndBatchOperator(&gtv, &ltv, 1, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE).setBmts(
        gotBmt ? &allBmts : nullptr)->execute()->_zis;
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
    return BoolLessBatchOperator::bmtCount(2 * num, width) + BoolAndBatchOperator::bmtCount(num, width);
}

bool BoolEqualBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}
