
#include "compute/single/arith/ArithOperator.h"

#include "ot/BaseOtOperator.h"
#include "comm/Comm.h"
#include "conf/Conf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

ArithOperator::ArithOperator(int64_t z, int width, int taskTag, int msgTagOffset,
                             int clientRank) : AbstractSingleOperator(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _zi = z;
    } else {
        if (Comm::rank() == clientRank) {
            int64_t z1 = Math::randInt();
            int64_t z0 = z - z1;
            auto r0 = Comm::sendAsync(z0, _width, 0, buildTag(_currentMsgTag));
            auto r1 = Comm::sendAsync(z1, _width, 1, buildTag(_currentMsgTag));
            Comm::wait(r0);
            Comm::wait(r1);
        } else if (Comm::isServer()) {
            Comm::receive(_zi, _width, clientRank, buildTag(_currentMsgTag));
        }
    }
}

ArithOperator::ArithOperator(int64_t x, int64_t y, int width, int taskTag, int msgTagOffset,
                             int clientRank) : AbstractSingleOperator(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = x;
        _yi = y;
    } else {
        if (Comm::rank() == clientRank) {
            int64_t x1 = Math::randInt();
            int64_t x0 = x - x1;
            int64_t y1 = Math::randInt();
            int64_t y0 = y - y1;
            std::vector xy0 = {x0, y0};
            std::vector xy1 = {x1, y1};
            auto r0 = Comm::sendAsync(xy0, _width, 0, buildTag(_currentMsgTag));
            auto r1 = Comm::sendAsync(xy1, _width, 1, buildTag(_currentMsgTag));
            Comm::wait(r0);
            Comm::wait(r1);
        } else if (Comm::isServer()) {
            std::vector<int64_t> temp;
            Comm::receive(temp, _width, clientRank, buildTag(_currentMsgTag));
            _xi = temp[0];
            _yi = temp[1];
        }
    }
}

ArithOperator *ArithOperator::execute() {
    throw std::runtime_error("Needs implementation.");
}

ArithOperator *ArithOperator::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        Comm::send(_zi, _width, clientRank, buildTag(_currentMsgTag));
    } else if (Comm::rank() == clientRank) {
        int64_t temp0, temp1;
        Comm::receive(temp0, _width, 0, buildTag(_currentMsgTag));
        Comm::receive(temp1, _width, 1, buildTag(_currentMsgTag));
        _result = ring(temp0 + temp1);
    }
    return this;
}
