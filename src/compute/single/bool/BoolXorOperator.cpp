
#include "parsec/compute/single/bool/BoolXorOperator.h"

#include "parsec/compute/single/arith/ArithAddOperator.h"

namespace parsec {

BoolXorOperator* BoolXorOperator::execute() {
    _zi = _xi ^ _yi;
    return this;
}

int BoolXorOperator::tagStride() { return 0; }

} // namespace parsec
