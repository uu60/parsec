//
// Created by 杜建璋 on 2024/9/6.
//

#include "compute/ArithExecutor.h"

#include <folly/futures/Future.h>

#include "ot/RsaOtExecutor.h"
#include "comm/IComm.h"
#include "utils/Math.h"

ArithExecutor::ArithExecutor(int64_t z, int l, int32_t objTag, int8_t msgTagOffset,
                             int clientRank) : AbstractSecureExecutor(l, objTag, msgTagOffset) {
    if (clientRank < 0) {
        _zi = ring(z);
    } else {
        // distribute operator
        if (IComm::impl->rank() == clientRank) {
            int64_t z1 = ring(Math::randInt());
            int64_t z0 = ring(z - z1);
            // To avoid long time waiting on network, send data in parallel.
            auto f0 = via(&System::_threadPool, [this, z0] {
                IComm::impl->send(&z0, 0, buildTag(_currentMsgTag));
            });
            auto f1 = via(&System::_threadPool, [this, z1] {
                IComm::impl->send(&z1, 1, buildTag(_currentMsgTag));
            });
            // Blocking wait. (sync)
            f0.wait();
            f1.wait();
            // Update tag.
            _currentMsgTag++;
        } else {
            // operator
            IComm::impl->receive(&_zi, clientRank, _currentMsgTag++);
        }
    }
}

ArithExecutor::ArithExecutor(int64_t x, int64_t y, int l, int32_t objTag, int8_t msgTagOffset,
                             int clientRank) : AbstractSecureExecutor(l, objTag, msgTagOffset) {
    if (clientRank < 0) {
        _xi = ring(x);
        _yi = ring(y);
    } else {
        auto msgTags = nextMsgTags(2);
        // distribute operator
        if (IComm::impl->rank() == clientRank) {
            int64_t x1 = ring(Math::randInt());
            int64_t x0 = ring(x - x1);
            int64_t y1 = ring(Math::randInt());
            int64_t y0 = ring(y - y1);
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
            // sync
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

ArithExecutor *ArithExecutor::execute() {
    throw std::runtime_error("This method cannot be called!");
}

std::string ArithExecutor::className() const {
    throw std::runtime_error("This method cannot be called!");
}

ArithExecutor *ArithExecutor::reconstruct(int clientRank) {
    if (IComm::impl->isServer()) {
        IComm::impl->send(&_zi, clientRank, buildTag(_currentMsgTag++));
    } else if (IComm::impl->rank() == clientRank) {
        int64_t z0, z1;
        IComm::impl->receive(&z0, 0, _currentMsgTag);
        IComm::impl->receive(&z1, 1, _currentMsgTag++);
        _result = ring(z0 + z1);
    }
    return this;
}
