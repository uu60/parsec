
#include "parsecdb/compute/batch/arith/ArithAddBatchOperator.h"
#include "parsecdb/utils/Math.h"

namespace parsecdb {

ArithAddBatchOperator* ArithAddBatchOperator::execute() {
    if (Comm::isClient()) {
        return this;
    }

    size_t n = _xis->size();
    _zis.resize(n);
    for (int i = 0; i < n; i++) {
        _zis[i] = Math::ringAdd((*_xis)[i], (*_yis)[i], _width);
    }

    return this;
}

int ArithAddBatchOperator::tagStride() { return 0; }

} // namespace parsecdb
