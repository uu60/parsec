//
// Created by 杜建璋 on 2024/12/2.
//

#include "compute/bool/BoolToArithExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/IComm.h"
#include "compute/arith/ArithExecutor.h"
#include "ot/RandOtExecutor.h"
#include "utils/Math.h"

BoolToArithExecutor *BoolToArithExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (IComm::impl->isServer()) {
        std::atomic_int64_t temp = 0;
        std::vector<std::future<void> > futures;
        futures.reserve(_l);

        bool isSender = IComm::impl->rank() == 0;
        for (int i = 0; i < _l; i++) {
            futures.push_back(System::_threadPool.push([this, isSender, &temp, i](int _) {
                int xb = static_cast<int>((_xi >> i) & 1);
                int64_t s0 = 0, s1 = 0;
                int64_t r = 0;
                if (isSender) {
                    // Sender
                    r = Math::randInt();
                    s0 = (xb << i) - r;
                    s1 = ((1 - xb) << i) - r;
                }
                RandOtExecutor e(0, s0, s1, xb, _l, _taskTag,
                                static_cast<int16_t>(_currentMsgTag + RandOtExecutor::needsMsgTags() * i));
                e.execute();
                if (isSender) {
                    temp += r;
                } else {
                    int64_t s_xb = e._result;
                    temp += s_xb;
                }
            }));
        }
        for (auto &f : futures) {
            f.wait();
        }
        _zi = ring(temp);
    }
    return this;
}

std::string BoolToArithExecutor::className() const {
    return "ToArithExecutor";
}

int16_t BoolToArithExecutor::needsMsgTags(int l) {
    return static_cast<int16_t>(RandOtExecutor::needsMsgTags() * l);
}

BoolToArithExecutor * BoolToArithExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    ArithExecutor e(_zi, _l, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (IComm::impl->rank() == clientRank) {
        _result = e._result;
    }
    return this;
}

/*
 * This is the method of Crypten to convert bool share to arith share.
 */
// ToArithE *ToArithE::execute() {
//     _currentMsgTag = _startMsgTag;
//     if (IComm::impl->isServer()) {
//         std::atomic_int64_t res = 0;
//         auto msgTags = nextMsgTags(_l);
//         std::vector<std::future<void> > futures;
//         futures.reserve(_l);
//
//         for (int i = 0; i < _l; i++) {
//             ABPair r = IntermediateDataSupport::pollABPairs(1)[0];
//             futures.push_back(System::_threadPool.push([this, i, &msgTags, &res, r](int _) {
//                 int64_t ri_b = r._b;
//                 int64_t ri_a = r._a;
//
//                 // Compute
//                 int64_t zi_b = ((_zi >> i) & 1) ^ ri_b;
//                 int64_t zo_b;
//
//                 // Decrypt
//                 IComm::impl->serverExchange(&zi_b, &zo_b, buildTag(msgTags[i]));
//                 int64_t z = zo_b ^ zi_b;
//
//                 // Compute
//                 res += (ri_a + z * IComm::impl->rank() - 2 * ri_a * z) << i;
//             }));
//         }
//         for (auto &f: futures) {
//             f.wait();
//         }
//         _zi = ring(res);
//     }
//     return this;
// }
