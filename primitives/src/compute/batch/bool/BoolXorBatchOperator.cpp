
#include "compute/batch/bool/BoolXorBatchOperator.h"

#include "accelerate/SimdSupport.h"

BoolXorBatchOperator * BoolXorBatchOperator::execute() {
    if (Conf::ENABLE_SIMD) {
        _zis = SimdSupport::xorV(*_xis, *_yis);
    } else {
        _zis.resize(_xis->size());
        for (int i = 0; i < _xis->size(); i++) {
            _zis[i] = (*_xis)[i] ^ (*_yis)[i];
        }
    }
    return this;
}

int BoolXorBatchOperator::tagStride() {
    return 0;
}

