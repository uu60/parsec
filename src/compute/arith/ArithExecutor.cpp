//
// Created by 杜建璋 on 2024/9/6.
//

#include "compute/arith/ArithExecutor.h"

#include "ot/BaseOtExecutor.h"
#include "comm/IComm.h"
#include "utils/Log.h"
#include "utils/Math.h"

ArithExecutor::ArithExecutor(int64_t z, int l, int16_t objTag, int16_t msgTagOffset,
                             int clientRank) : AbstractSecureExecutor(l, objTag, msgTagOffset) {
    if (clientRank < 0) {
        _zi = z;
    } else {
        // distribute operator
        if (IComm::impl->rank() == clientRank) {
            int64_t z1 = Math::randInt();
            int64_t z0 = z - z1;
            // To avoid long time waiting on network, send data in parallel.
            auto f0 = System::_threadPool.push([this, z0](int _) {
                IComm::impl->send(&z0, 0, buildTag(_currentMsgTag));
            });
            auto f1 = System::_threadPool.push([this, z1](int _) {
                IComm::impl->send(&z1, 1, buildTag(_currentMsgTag));
            });
            // Blocking wait. (sync)
            f0.wait();
            f1.wait();
        } else {
            // operator
            IComm::impl->receive(&_zi, clientRank, buildTag(_currentMsgTag));
        }
    }
}

ArithExecutor::ArithExecutor(int64_t x, int64_t y, int l, int16_t objTag, int16_t msgTagOffset,
                             int clientRank) : AbstractSecureExecutor(l, objTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = x;
        _yi = y;
    } else {
        auto msgTags = nextMsgTags(2);
        // distribute operator
        if (IComm::impl->rank() == clientRank) {
            int64_t x1 = Math::randInt();
            int64_t x0 = x - x1;
            int64_t y1 = Math::randInt();
            int64_t y0 = y - y1;
            std::vector<std::future<void> > futures;
            futures.reserve(4);
            futures.push_back(System::_threadPool.push([x0, this, msgTags](int _) {
                IComm::impl->send(&x0, 0, buildTag(msgTags[0]));
            }));
            futures.push_back(System::_threadPool.push([y0, this, msgTags](int _) {
                IComm::impl->send(&y0, 0, buildTag(msgTags[1]));
            }));
            futures.push_back(System::_threadPool.push([x1, this, msgTags](int _) {
                IComm::impl->send(&x1, 1, buildTag(msgTags[0]));
            }));
            futures.push_back(System::_threadPool.push([y1, this, msgTags](int _) {
                IComm::impl->send(&y1, 1, buildTag(msgTags[1]));
            }));
            // sync
            for (auto &f : futures) {
                f.wait();
            }
        } else {
            // operator
            IComm::impl->receive(&_xi, clientRank, buildTag(msgTags[0]));
            IComm::impl->receive(&_yi, clientRank, buildTag(msgTags[1]));
        }
    }
}

ArithExecutor *ArithExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string ArithExecutor::className() const {
    throw std::runtime_error("This method cannot be called!");
}

ArithExecutor *ArithExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_zi, clientRank, buildTag(_currentMsgTag));
    } else if (IComm::impl->rank() == clientRank) {
        int64_t z0, z1;
        IComm::impl->receive(&z0, 0, buildTag(_currentMsgTag));
        IComm::impl->receive(&z1, 1, buildTag(_currentMsgTag));
        _result = ring(z0 + z1);
    }
    return this;
}
