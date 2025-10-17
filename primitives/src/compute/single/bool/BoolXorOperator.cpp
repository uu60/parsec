
#include "compute/single/bool/BoolXorOperator.h"

#include "compute/single/arith/ArithAddOperator.h"

BoolXorOperator *BoolXorOperator::execute() {
    _zi = _xi ^ _yi;
    return this;
}

int BoolXorOperator::tagStride() {
    return 0;
}
