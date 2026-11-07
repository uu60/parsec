
#include "parsecdb/compute/single/arith/ArithAddOperator.h"
#include "parsecdb/comm/Comm.h"
#include "parsecdb/utils/Math.h"
#include "parsecdb/utils/System.h"
#include <limits>

namespace parsecdb {

ArithAddOperator* ArithAddOperator::execute() {
    if (Comm::isServer()) {
        _zi = Math::ringAdd(_xi, _yi, _width);
    }

    return this;
}

} // namespace parsecdb
