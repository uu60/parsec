
#include "parsecdb/compute/single/bool/BoolXorOperator.h"

#include "parsecdb/compute/single/arith/ArithAddOperator.h"

namespace parsecdb {

BoolXorOperator* BoolXorOperator::execute() {
    _zi = _xi ^ _yi;
    return this;
}

int BoolXorOperator::tagStride() { return 0; }

} // namespace parsecdb
