
#include "compute/batch/arith/ArithAddBatchOperator.h"

ArithAddBatchOperator * ArithAddBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    size_t n = _xis->size();
    _zis.resize(n);
    for (int i = 0; i < n; i++) {
        _zis[i] = (*_xis)[i] + (*_yis)[i];
    }

    return this;
}

int ArithAddBatchOperator::tagStride() {
    return 0;
}
