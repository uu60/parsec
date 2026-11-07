
#include "parsec/compute/single/arith/ArithAddOperator.h"
#include "parsec/comm/Comm.h"
#include "parsec/utils/Math.h"
#include "parsec/utils/System.h"
#include <limits>

namespace parsec {

ArithAddOperator* ArithAddOperator::execute() {
    if (Comm::isServer()) {
        _zi = Math::ringAdd(_xi, _yi, _width);
    }

    return this;
}

} // namespace parsec
