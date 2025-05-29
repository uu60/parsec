//
// Created by 杜建璋 on 25-5-25.
//

#include "../../../../include/compute/batch/bool/BoolEqualBatchOperator.h"

#include "compute/batch/bool/BoolAndBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"

BoolEqualBatchOperator *BoolEqualBatchOperator::execute() {
    std::vector<BitwiseBmt> allBmts;
    bool gotBmt = prepareBmts(allBmts);

    std::vector<BitwiseBmt> bmts;
    if (gotBmt) {
        int bc = BoolLessBatchOperator::bmtCount(_xis->size() * 2, _width);
        bmts = std::vector(allBmts.end() - bc, allBmts.end());
        allBmts.resize(allBmts.size() - bc);
    }

    auto gtv_ltv = BoolLessBatchOperator(_xis, _yis, _width, 0, _currentMsgTag)
            .setBmts(gotBmt ? &bmts : nullptr)->execute()->_zis;
    for (auto &v: gtv_ltv) {
        v = v ^ Comm::rank();
    }
    std::vector ltv(gtv_ltv.begin() + gtv_ltv.size() / 2, gtv_ltv.end());
    std::vector gtv = std::move(gtv_ltv);
    gtv.resize(gtv.size() / 2);

    _zis = BoolAndBatchOperator(&gtv, &ltv, 1, 0, _currentMsgTag,
                                SecureOperator::NO_CLIENT_COMPUTE).setBmts(gotBmt ? &allBmts : nullptr)->execute()->
            _zis;
    return this;
}

BoolEqualBatchOperator *BoolEqualBatchOperator::setBmts(std::vector<BitwiseBmt> *bmts) {
    this->_bmts = bmts;
    return this;
}

int BoolEqualBatchOperator::msgTagCount() {
    return std::max(BoolLessBatchOperator::msgTagCount(), BoolAndBatchOperator::msgTagCount());
}

int BoolEqualBatchOperator::bmtCount(int num, int width) {
    return BoolLessBatchOperator::bmtCount(num, width) + BoolAndBatchOperator::bmtCount(num, width);
}

bool BoolEqualBatchOperator::prepareBmts(std::vector<BitwiseBmt> &bmts) {
    if (_bmts != nullptr) {
        bmts = std::move(*_bmts);
        return true;
    }
    return false;
}
