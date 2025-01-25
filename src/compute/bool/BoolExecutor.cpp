//
// Created by 杜建璋 on 2024/11/7.
//

#include "compute/bool/BoolExecutor.h"

#include "comm/Comm.h"
#include "utils/Math.h"
#include "ot/BaseOtExecutor.h"
#include "utils/Log.h"
#include "utils/System.h"

BoolExecutor::BoolExecutor(int64_t z, int l, int16_t taskTag, int16_t msgTagOffset,
                           int clientRank) : AbstractSecureExecutor(
    l, taskTag, msgTagOffset) {
    if (clientRank < 0) {
        _zi = ring(z);
    } else {
        // distribute operator
        if (Comm::rank() == clientRank) {
            int64_t z1 = ring(Math::randInt());
            int64_t z0 = ring(z ^ z1);

            std::vector zv0 = {z0};
            std::vector zv1 = {z1};
            auto f = System::_threadPool.push([&] (int) {
                Comm::send(zv0, 0, buildTag(_currentMsgTag));
            });
            Comm::send(zv1, 1, buildTag(_currentMsgTag));
            f.wait();
        } else {
            // operator
            std::vector<int64_t> temp;
            Comm::receive(temp, clientRank, buildTag(_currentMsgTag));
            _zi = temp[0];
        }
    }
}

BoolExecutor::BoolExecutor(int64_t x, int64_t y, int l, int16_t taskTag, int16_t msgTagOffset,
                           int clientRank) : AbstractSecureExecutor(l, taskTag, msgTagOffset) {
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
            auto f = System::_threadPool.push([this, &xy0](int _) {
                Comm::send(xy0, 0, buildTag(_currentMsgTag));
            });
            Comm::send(xy1, 1, buildTag(_currentMsgTag));
            // sync
            f.wait();
        } else {
            // operator
            std::vector<int64_t> temp;
            Comm::receive(temp, clientRank, buildTag(_currentMsgTag));
            _xi = temp[0];
            _yi = temp[1];
        }
    }
}

BoolExecutor *BoolExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
        std::vector temp = {_zi};
        Comm::send(temp, clientRank, buildTag(_currentMsgTag));
    } else if (Comm::rank() == clientRank) {
        std::vector<int64_t> temp0, temp1;
        Comm::receive(temp0, 0, buildTag(_currentMsgTag));
        Comm::receive(temp1, 1, buildTag(_currentMsgTag));
        _result = ring(temp0[0] ^ temp1[0]);
    }
    return this;
}

BoolExecutor *BoolExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string BoolExecutor::className() const {
    throw std::runtime_error("This method cannot be called!");
}
