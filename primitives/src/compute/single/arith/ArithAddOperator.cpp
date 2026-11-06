
#include "compute/single/arith/ArithAddOperator.h"
#include "comm/Comm.h"
#include "utils/System.h"
#include "utils/Math.h"
#include <limits>

namespace parsec {


ArithAddOperator *ArithAddOperator::execute() {
    if (Comm::isServer()) {
        _zi = Math::ringAdd(_xi, _yi, _width);
    }

    return this;
}

} // namespace parsec
