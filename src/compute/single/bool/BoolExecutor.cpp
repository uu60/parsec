//
// Created by 杜建璋 on 2024/11/7.
//

#include "compute/single/bool/BoolExecutor.h"

#include "comm/Comm.h"
#include "conf/Conf.h"
#include "utils/Math.h"
#include "ot/BaseOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/System.h"

BoolExecutor::BoolExecutor(int64_t z, int l, int taskTag, int msgTagOffset,
                           int clientRank) : AbstractSingleExecutor(
    l, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _zi = ring(z);
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            int64_t z1 = ring(Math::randInt());
            int64_t z0 = ring(z ^ z1);

            std::future<void> f;
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                f = ThreadPoolSupport::submit([&] {
                    Comm::send(z0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(z0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(z1, _width, 1, buildTag(_currentMsgTag));
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                f.wait();
            }
        } else {
            // operator
            Comm::receive(_zi, _width, clientRank, buildTag(_currentMsgTag));
        }
    }
}

BoolExecutor::BoolExecutor(int64_t x, int64_t y, int l, int taskTag, int msgTagOffset,
                           int clientRank) : AbstractSingleExecutor(l, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = ring(x);
        _yi = ring(y);
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            int64_t x1 = ring(Math::randInt());
            int64_t x0 = ring(x ^ x1);
            int64_t y1 = ring(Math::randInt());
            int64_t y0 = ring(y ^ y1);
            std::vector xy0 = {x0, y0};
            std::vector xy1 = {x1, y1};
            std::future<void> f;
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                f = ThreadPoolSupport::submit([this, &xy0] {
                    Comm::send(xy0, _width, 0, buildTag(_currentMsgTag));
                });
            } else {
                Comm::send(xy0, _width, 0, buildTag(_currentMsgTag));
            }
            Comm::send(xy1, _width, 1, buildTag(_currentMsgTag));
            // sync
            if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
                f.wait();
            }
        } else {
            // operator
            std::vector<int64_t> temp;
            Comm::receive(temp, _width, clientRank, buildTag(_currentMsgTag));
            _xi = temp[0];
            _yi = temp[1];
        }
    }
}

BoolExecutor *BoolExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        Comm::send(_zi, _width, clientRank, buildTag(_currentMsgTag));
    } else if (Comm::rank() == clientRank) {
        int64_t temp0, temp1;
        Comm::receive(temp0, _width, 0, buildTag(_currentMsgTag));
        Comm::receive(temp1, _width, 1, buildTag(_currentMsgTag));
        _result = ring(temp0 ^ temp1);
    }
    return this;
}

BoolExecutor *BoolExecutor::execute() {
    throw std::runtime_error("This method needs implementation.");
}
