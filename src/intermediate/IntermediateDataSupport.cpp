//
// Created by 杜建璋 on 2024/11/25.
//

#include "intermediate//IntermediateDataSupport.h"

#include "comm/Comm.h"
#include "intermediate/ABPairGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "ot/BaseOtExecutor.h"
#include "utils/Log.h"
#include "utils/Math.h"

void IntermediateDataSupport::offerBmt(Bmt bmt) {
    _bmts->offer(bmt);
}

// void IntermediateDataSupport::offerABPair(ABPair pair) {
//     _pairs.offer(pair);
// }

void IntermediateDataSupport::prepareRot() {
    if (Comm::isServer()) {
        std::vector<std::future<void> > futures;
        futures.reserve(2);

        for (int i = 0; i < 2; i++) {
            futures.push_back(System::_threadPool.push([i](int _) {
                bool isSender = Comm::rank() == i;
                if (isSender) {
                    _sRot = new SRot();
                    _sRot->_r0 = Math::randInt();
                    _sRot->_r1 = Math::randInt();
                } else {
                    _rRot = new RRot();
                    _rRot->_b = static_cast<int>(Math::randInt(0, 1));;
                }
                BaseOtExecutor e(i, isSender ? _sRot->_r0 : -1, isSender ? _sRot->_r1 : -1, !isSender ? _rRot->_b : -1,
                                 64, 2, i * BaseOtExecutor::needMsgTags());
                e.execute();
                if (!isSender) {
                    _rRot->_rb = e._result;
                }
            }));
        }

        for (auto &f: futures) {
            f.wait();
        }
    }
}

std::vector<Bmt> IntermediateDataSupport::pollBmts(int count, int l) {
    std::vector<Bmt> result;
    if (Comm::isServer()) {
        result.reserve(count);

        while (count > 0) {
            int left = currentBmtLeftTimes;

            if (currentBmt == nullptr || left == 0) {
                delete currentBmt;
                Bmt newBmt = _bmts->poll();
                newBmt._a = Math::ring(newBmt._a, l);
                newBmt._b = Math::ring(newBmt._b, l);
                newBmt._c = Math::ring(newBmt._c, l);
                currentBmt = new Bmt(newBmt);
                currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
                left = Conf::BMT_USAGE_LIMIT;
            }

            int useCount = std::min(count, left);

            for (int i = 0; i < useCount; ++i) {
                result.push_back(*currentBmt);
            }

            currentBmtLeftTimes -= useCount;
            count -= useCount;
        }
    }

    return result;
}

// std::vector<ABPair> IntermediateDataSupport::pollABPairs(int num) {
//     std::vector<ABPair> ret;
//     if (Comm::isServer()) {
//         ret.reserve(num);
//         for (int i = 0; i < num; i++) {
//             ret.push_back(_pairs.poll());
//         }
//     }
//     return ret;
// }

void IntermediateDataSupport::startGenerateBmtsAsync() {
    if (Comm::isServer() && Conf::INTERM_PREGENERATED) {
        System::_threadPool.push([](int _) {
            while (!System::_shutdown.load()) {
                offerBmt(BmtGenerator(64, 0, 0).execute()->_bmt);
            }
        });
    }
}

// void IntermediateDataSupport::startGenerateABPairsAsyc() {
//     if (Comm::isServer()) {
//         System::_threadPool.push([](int _) {
//             while (!System::_shutdown.load()) {
//                 offerABPair(ABPairGenerator::getInstance().execute()->_pair);
//             }
//         });
//     }
// }
