//
// Created by 杜建璋 on 2024/12/2.
//

#include "compute/bool/BoolToArithExecutor.h"

#include "intermediate/ABPair.h"
#include "intermediate/IntermediateDataSupport.h"
#include "comm/IComm.h"

BoolToArithExecutor *BoolToArithExecutor::execute() {
    std::atomic_int64_t res = 0;
    auto msgTags = nextMsgTags(_l);
    std::vector<std::future<void>> futures;
    futures.reserve(_l);

    for (int i = 0; i < _l; i++) {
        futures.push_back(System::_threadPool.push([this, i, msgTags, &res] (int _) {
            ABPair r = IntermediateDataSupport::pollABPairs(1)[0];
            int64_t ri_b = r._b;
            int64_t ri_a = r._a;

            // Compute
            int64_t zi_b = ((_zi >> i) & 1) ^ ri_b;
            int64_t zo_b;

            // Decrypt
            IComm::impl->serverExchange(&zi_b, &zo_b, buildTag(msgTags[i]));
            int64_t z = zo_b ^ zi_b;

            // Compute
            res += (ri_a + z * IComm::impl->rank() - 2 * ri_a * z) << i;
        }));
    }
    for (auto &f : futures) {
        f.wait();
    }
    _zi = res;
    return this;
}

std::string BoolToArithExecutor::className() const {
    return "ToArithExecutor";
}

int16_t BoolToArithExecutor::neededMsgTags(int l) {
    return static_cast<int16_t>(l);
}

/*
 * This is the method of ABY to convert bool share to arith share.
 */
// int64_t BoolExecutor::arithZi() const {
//     int64_t xa = 0;
//     if (IComm::impl->isServer()) {
//         bool isSender = IComm::impl->rank() == 0;
//         for (int i = 0; i < _l; i++) {
//             int xb = static_cast<int>((_xi >> i) & 1);
//             int64_t s0 = 0, s1 = 0;
//             int64_t r = 0;
//             if (isSender) {
//                 // Sender
//                 r = ring(Math::randInt());
//                 s0 = ring((xb << i) - r);
//                 s1 = ring(((1 - xb) << i) - r);
//             }
//             RsaOtExecutor e(0, s0, s1, _l, xb);
//             e.execute();
//             if (isSender) {
//                 xa = ring(xa + r);
//             } else {
//                 int64_t s_xb = e._result;
//                 xa = ring(xa + s_xb);
//             }
//         }
//     }
//     return xa;
// }
