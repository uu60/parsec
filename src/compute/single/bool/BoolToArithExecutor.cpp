//
// Created by 杜建璋 on 2024/12/2.
//

#include "compute/single/bool/BoolToArithExecutor.h"

#include "intermediate/IntermediateDataSupport.h"
#include "comm/Comm.h"
#include "compute/single/arith/ArithExecutor.h"
#include "ot/RandOtBatchExecutor.h"
#include "ot/RandOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Math.h"

BoolToArithExecutor *BoolToArithExecutor::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isClient()) {
        return this;
    }

    std::atomic_int64_t temp = 0;
    bool isSender = Comm::rank() == 0;

    std::vector<std::future<void> > futures;
    futures.reserve(_width);

    if constexpr (Conf::TASK_BATCHING) {
        std::vector<int64_t> ss0, ss1;
        std::vector<int> choices;
        std::vector<int64_t> rs;

        if (isSender) {
            ss0.reserve(_width);
            ss1.reserve(_width);
            rs.reserve(_width);
        } else {
            choices.reserve(_width);
        }

        for (int i = 0; i < _width; i++) {
            int xb = static_cast<int>((_xi >> i) & 1);
            if (isSender) {
                int64_t r = Math::randInt();
                rs.push_back(r);
                int64_t s0 = (static_cast<int64_t>(xb) << i) - r;
                int64_t s1 = (static_cast<int64_t>(1 - xb) << i) - r;
                ss0.push_back(s0);
                ss1.push_back(s1);
            } else {
                choices.push_back(xb);
            }
        }

        RandOtBatchExecutor e(0, &ss0, &ss1, &choices, _width, _taskTag, _currentMsgTag);
        e.execute();

        if (isSender) {
            for (auto r: rs) {
                temp += r;
            }
        } else {
            for (int i = 0; i < _width; ++i) {
                temp += e._results[i];
            }
        }
    } else {
        auto process = [&](int i) {
            int xb = static_cast<int>((_xi >> i) & 1);
            int64_t s0 = 0, s1 = 0;
            int64_t r = 0;
            if (isSender) {
                // Sender
                r = Math::randInt();
                s0 = (static_cast<int64_t>(xb) << i) - r;
                s1 = (static_cast<int64_t>(1 - xb) << i) - r;
            }
            RandOtExecutor e(0, s0, s1, xb, _width, _taskTag,
                             static_cast<int>(_currentMsgTag + RandOtExecutor::msgTagCount(_width) * i));
            e.execute();
            if (isSender) {
                temp += r;
            } else {
                temp += e._result;
            }
        };
        if constexpr (Conf::INTRA_OPERATOR_PARALLELISM) {
            for (int i = 0; i < _width; i++) {
                futures.push_back(ThreadPoolSupport::submit([&, i] {
                    process(i);
                }));
            }
        } else {
            for (int i = 0; i < _width; i++) {
                process(i);
            }
        }
    }
    for (auto &f: futures) {
        f.wait();
    }
    _zi = ring(temp);
    return this;
}

int BoolToArithExecutor::msgTagCount(int width) {
    return static_cast<int>(RandOtExecutor::msgTagCount(width) * width);
}

BoolToArithExecutor *BoolToArithExecutor::reconstruct(int clientRank) {
    _currentMsgTag = _startMsgTag;
    ArithExecutor e(_zi, _width, _taskTag, _currentMsgTag, NO_CLIENT_COMPUTE);
    e.reconstruct(clientRank);
    if (Comm::rank() == clientRank) {
        _result = e._result;
    }
    return this;
}

/*
 * This is the method of Crypten to convert bool share to arith share.
 */
// ToArithE *ToArithE::execute() {
//     _currentMsgTag = _startMsgTag;
//     if (IComm::isServer()) {
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
//                 IComm::serverExchange(&zi_b, &zo_b, buildTag(msgTags[i]));
//                 int64_t z = zo_b ^ zi_b;
//
//                 // Compute
//                 res += (ri_a + z * IComm::rank() - 2 * ri_a * z) << i;
//             }));
//         }
//         for (auto &f: futures) {
//             f.wait();
//         }
//         _zi = ring(res);
//     }
//     return this;
// }
