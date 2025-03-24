//
// Created by 杜建璋 on 2024/11/25.
//

#include "intermediate//IntermediateDataSupport.h"

#include "comm/Comm.h"
#include "intermediate/ABPairGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "ot/BaseOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"

void IntermediateDataSupport::offerBmt(Bmt bmt) {
    _bmts->offer(bmt);
}

void IntermediateDataSupport::offerBitwiseBmt(BitwiseBmt bmt) {
    _bitwiseBmts->offer(bmt);
}

void IntermediateDataSupport::prepareBmt() {
    if constexpr (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        _fixedBmt = BmtGenerator(64, 0, 0).execute()->_bmt;
        _fixedBitwiseBmt = BitwiseBmtGenerator(64, 0, 0).execute()->_bmt;
    } else if constexpr (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        if constexpr (Conf::BMT_QUEUE_TYPE == Conf::LOCK_QUEUE) {
            _bmts = new LockBlockingQueue<Bmt>(Conf::MAX_BMTS);
        } else {
            _bmts = new BoostLockFreeQueue<Bmt>(Conf::MAX_BMTS);
        }

        if constexpr (Conf::BMT_QUEUE_TYPE == Conf::LOCK_QUEUE) {
            _bitwiseBmts = new LockBlockingQueue<BitwiseBmt>(Conf::MAX_BMTS);
        } else {
            _bitwiseBmts = new BoostLockFreeQueue<BitwiseBmt>(Conf::MAX_BMTS);
        }

        startGenerateBmtsAsync();
        startGenerateBitwiseBmtsAsync();
    }
}

void IntermediateDataSupport::init() {
    if (Comm::isClient()) {
        return;
    }

    prepareRot();

    prepareBmt();
}

// void IntermediateDataSupport::offerABPair(ABPair pair) {
//     _pairs.offer(pair);
// }

void IntermediateDataSupport::prepareRot() {
    if (Comm::isClient()) {
        return;
    }

    for (int i = 0; i < 2; i++) {
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
                         64, 2, i * BaseOtExecutor::msgTagCount());
        e.execute();
        if (!isSender) {
            _rRot->_rb = e._result;
        }
    }
}

std::vector<Bmt> IntermediateDataSupport::pollBmts(int count, int width) {
    std::vector<Bmt> result;
    if (Comm::isClient()) {
        return result;
    }

    result.reserve(count);
    if constexpr (Conf::BMT_USAGE_LIMIT == 1) {
        result.push_back(_bmts->poll());
        return result;
    }

    while (count > 0) {
        int left = _currentBmtLeftTimes;

        if (_currentBmt == nullptr || left == 0) {
            delete _currentBmt;
            Bmt newBmt = _bmts->poll();
            newBmt._a = Math::ring(newBmt._a, width);
            newBmt._b = Math::ring(newBmt._b, width);
            newBmt._c = Math::ring(newBmt._c, width);
            _currentBmt = new Bmt(newBmt);
            _currentBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
            left = Conf::BMT_USAGE_LIMIT;
        }

        int useCount = std::min(count, left);

        for (int i = 0; i < useCount; ++i) {
            result.push_back(*_currentBmt);
        }

        _currentBmtLeftTimes -= useCount;
        count -= useCount;
    }

    return result;
}

std::vector<BitwiseBmt> IntermediateDataSupport::pollBitwiseBmts(int count, int width) {
    std::vector<BitwiseBmt> result;
    if (Comm::isClient()) {
        return result;
    }

    result.reserve(count);
    if constexpr (Conf::BMT_USAGE_LIMIT == 1) {
        result.push_back(_bitwiseBmts->poll());
        return result;
    }

    while (count > 0) {
        int left = _currentBitwiseBmtLeftTimes;

        if (_currentBitwiseBmt == nullptr || left == 0) {
            delete _currentBitwiseBmt;
            BitwiseBmt newBmt = _bitwiseBmts->poll();
            newBmt._a = Math::ring(newBmt._a, width);
            newBmt._b = Math::ring(newBmt._b, width);
            newBmt._c = Math::ring(newBmt._c, width);
            _currentBitwiseBmt = new BitwiseBmt(newBmt);
            _currentBitwiseBmtLeftTimes = Conf::BMT_USAGE_LIMIT;
            left = Conf::BMT_USAGE_LIMIT;
        }

        int useCount = std::min(count, left);

        for (int i = 0; i < useCount; ++i) {
            result.push_back(*_currentBitwiseBmt);
        }

        _currentBitwiseBmtLeftTimes -= useCount;
        count -= useCount;
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
    if (Comm::isServer() && Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        ThreadPoolSupport::submit([] {
            while (!System::_shutdown.load()) {
                offerBmt(BmtGenerator(8, 0, 0).execute()->_bmt);
            }
        });
    }
}

void IntermediateDataSupport::startGenerateBitwiseBmtsAsync() {
    if (Comm::isServer() && Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        ThreadPoolSupport::submit([] {
            while (!System::_shutdown.load()) {
                offerBitwiseBmt(BitwiseBmtGenerator(64, 1, 0).execute()->_bmt);
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
