//
// Created by 杜建璋 on 2024/9/6.
//

#include "compute/single/arith/ArithExecutor.h"

#include "ot/BaseOtExecutor.h"
#include "comm/Comm.h"
#include "conf/Conf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

ArithExecutor::ArithExecutor(int64_t z, int width, int16_t taskTag, int16_t msgTagOffset,
                             int clientRank) : AbstractSingleExecutor(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _zi = z;
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            int64_t z1 = Math::randInt();
            int64_t z0 = z - z1;
            std::future<void> f;
            if (Conf::INTRA_OPERATOR_PARALLELISM) {
                // To avoid long time waiting on network, send data in parallel.
                f = ThreadPoolSupport::submit([&] {
                    Comm::send(z0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(z0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(z1, _width, 1, buildTag(_currentMsgTag));
            if (Conf::INTRA_OPERATOR_PARALLELISM) {
                f.wait();
            }
        } else if (Comm::isServer()) {
            // operator
            Comm::receive(_zi, _width, clientRank, buildTag(_currentMsgTag));
        }
    }
}

ArithExecutor::ArithExecutor(int64_t x, int64_t y, int width, int16_t taskTag, int16_t msgTagOffset,
                             int clientRank) : AbstractSingleExecutor(width, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = x;
        _yi = y;
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            int64_t x1 = Math::randInt();
            int64_t x0 = x - x1;
            int64_t y1 = Math::randInt();
            int64_t y0 = y - y1;
            std::vector xy0 = {x0, y0};
            std::vector xy1 = {x1, y1};
            std::future<void> f;
            if (Conf::INTRA_OPERATOR_PARALLELISM) {
                f = ThreadPoolSupport::submit([&] {
                    Comm::send(xy0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(xy0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(xy1, _width, 1, buildTag(_currentMsgTag));
            if (Conf::INTRA_OPERATOR_PARALLELISM) {
                // sync
                f.wait();
            }
        } else if (Comm::isServer()) {
            // operator
            std::vector<int64_t> temp;
            Comm::receive(temp, _width, clientRank, buildTag(_currentMsgTag));
            _xi = temp[0];
            _yi = temp[1];
        }
    }
}

ArithExecutor *ArithExecutor::execute() {
    throw std::runtime_error("Needs implementation.");
}

ArithExecutor *ArithExecutor::reconstruct(int clientRank) {
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
