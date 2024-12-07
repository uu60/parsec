//
// Created by 杜建璋 on 2024/11/7.
//

#include "compute/BoolExecutor.h"

#include <folly/futures/Future.h>

#include "comm/IComm.h"
#include "utils/Math.h"
#include "ot/RsaOtExecutor.h"
#include "utils/System.h"

BoolExecutor::BoolExecutor(int64_t z, int l, int32_t objTag, int8_t msgTagOffset,
                           int clientRank) : AbstractSecureExecutor(
    l, objTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = ring(z);
    } else {
        // distribute operator
        if (IComm::impl->isClient()) {
            int64_t z1 = ring(Math::randInt());
            int64_t z0 = ring(z ^ z1);
            auto f0 = via(&System::_threadPool, [this, z0] {
                IComm::impl->send(&z0, 0, buildTag(_currentMsgTag));
            });
            auto f1 = via(&System::_threadPool, [this, z1] {
                IComm::impl->send(&z1, 1, buildTag(_currentMsgTag));
            });
            f0.wait();
            f1.wait();
            _currentMsgTag++;
        } else {
            // operator
            IComm::impl->receive(&_xi, clientRank, _currentMsgTag++);
        }
    }
}

BoolExecutor::BoolExecutor(int64_t x, int64_t y, int l, int32_t objTag, int8_t msgTagOffset,
                           int clientRank) : AbstractSecureExecutor(l, objTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = ring(x);
        _yi = ring(y);
    } else {
        auto msgTags = nextMsgTags(2);
        // distribute operator
        if (IComm::impl->rank() == clientRank) {
            int64_t x1 = ring(Math::randInt());
            int64_t x0 = ring(x ^ x1);
            int64_t y1 = ring(Math::randInt());
            int64_t y0 = ring(y ^ y1);
            std::vector<folly::Future<folly::Unit> > futures(4);
            futures.push_back(via(&System::_threadPool, [x0, this, msgTags] {
                IComm::impl->send(&x0, 0, buildTag(msgTags[0]));
            }));
            futures.push_back(via(&System::_threadPool, [y0, this, msgTags] {
                IComm::impl->send(&y0, 0, buildTag(msgTags[1]));
            }));
            futures.push_back(via(&System::_threadPool, [x1, this, msgTags] {
                IComm::impl->send(&x1, 1, buildTag(msgTags[0]));
            }));
            futures.push_back(via(&System::_threadPool, [y1, this, msgTags] {
                IComm::impl->send(&y1, 1, buildTag(msgTags[1]));
            }));
            for (auto &f: futures) {
                f.wait();
            }
        } else {
            // operator
            IComm::impl->receive(&_xi, clientRank, msgTags[0]);
            IComm::impl->receive(&_yi, clientRank, msgTags[1]);
        }
    }
}

BoolExecutor *BoolExecutor::reconstruct(int clientRank) {
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_xi, clientRank, _currentMsgTag++);
    } else {
        int64_t z0, z1;
        IComm::impl->receive(&z0, 0, _currentMsgTag);
        IComm::impl->receive(&z1, 1, _currentMsgTag++);
        _result = z0 ^ z1;
    }
    return this;
}

BoolExecutor *BoolExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string BoolExecutor::className() const {
    throw std::runtime_error("This method cannot be called!");
}
