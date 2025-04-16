//
// Created by 杜建璋 on 2024/11/25.
//

#include "intermediate/IntermediateDataSupport.h"

#include "comm/Comm.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "ot/BaseOtExecutor.h"
#include "parallel/ThreadPoolSupport.h"
#include "sync/BoostLockFreeQueue.h"
#include "sync/BoostSpscQueue.h"
#include "sync/LockBlockingQueue.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include <climits>

void IntermediateDataSupport::prepareBmt() {
    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        _fixedBmt = BmtGenerator(64, 0, 0).execute()->_bmt;
        _fixedBitwiseBmt = BitwiseBmtGenerator(64, 0, 0).execute()->_bmt;
    } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        _bitwiseBmtQs.resize(Conf::BMT_QUEUE_NUM);
        if (Conf::BMT_QUEUE_TYPE == Conf::LOCK_QUEUE) {
            for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
                _bitwiseBmtQs[i] = new LockBlockingQueue<BitwiseBmt>(Conf::MAX_BMTS);
            }
        } else if (Conf::BMT_QUEUE_TYPE == Conf::LOCK_FREE_QUEUE) {
            for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
                _bitwiseBmtQs[i] = new BoostLockFreeQueue<BitwiseBmt>(Conf::MAX_BMTS);
            }
        } else {
            for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
                _bitwiseBmtQs[i] = new BoostSPSCQueue<BitwiseBmt, INT_MAX>();
            }
        }
        startGenerateBitwiseBmtsAsync();

        if (Conf::DISABLE_ARITH) {
            return;
        }

        _bmtQs.resize(Conf::BMT_QUEUE_NUM);
        if (Conf::BMT_QUEUE_TYPE == Conf::LOCK_QUEUE) {
            for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
                _bmtQs[i] = new LockBlockingQueue<Bmt>(Conf::MAX_BMTS);
            }
        } else if (Conf::BMT_QUEUE_TYPE == Conf::LOCK_FREE_QUEUE) {
            for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
                _bmtQs[i] = new BoostLockFreeQueue<Bmt>(Conf::MAX_BMTS);
            }
        } else {
            for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
                _bmtQs[i] = new BoostSPSCQueue<Bmt, INT_MAX>();
            }
        }
        startGenerateBmtsAsync();
    }
}


void IntermediateDataSupport::init() {
    if (Comm::isClient()) {
        return;
    }

    prepareRot();

    prepareBmt();

    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND && Conf::BMT_PRE_GEN_SECONDS > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(Conf::BMT_PRE_GEN_SECONDS));
    }
}

void IntermediateDataSupport::finalize() {
    delete _currentBmt;
    delete _currentBitwiseBmt;
    delete _sRot;
    delete _rRot;
}

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

    while (count > 0) {
        int left = _currentBmtLeftTimes;

        if (_currentBmt == nullptr || left == 0) {
            delete _currentBmt;
            Bmt newBmt = _bmtQs[_currentBmtQ++ % Conf::BMT_QUEUE_NUM]->poll();
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

    while (count > 0) {
        int left = _currentBitwiseBmtLeftTimes;

        if (_currentBitwiseBmt == nullptr || left == 0) {
            delete _currentBitwiseBmt;
            BitwiseBmt newBmt = _bitwiseBmtQs[_currentBitwiseBmtQ++ % Conf::BMT_QUEUE_NUM]->poll();
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

void IntermediateDataSupport::startGenerateBmtsAsync() {
    if (Comm::isServer() && Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
            ThreadPoolSupport::submit([i] {
                auto q = _bmtQs[i];
                while (!System::_shutdown.load()) {
                    q->offer(BmtGenerator(64, Conf::BMT_QUEUE_NUM + i, 0).execute()->_bmt);
                }
            });
        }
    }
}

void IntermediateDataSupport::startGenerateBitwiseBmtsAsync() {
    if (Comm::isServer() && Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
            ThreadPoolSupport::submit([i] {
                auto q = _bitwiseBmtQs[i];
                while (!System::_shutdown.load()) {
                    auto bitwiseBmts = BitwiseBmtBatchGenerator(Conf::BMT_GEN_BATCH_SIZE, 64, i, 0).execute()->_bmts;
                    for (auto b: bitwiseBmts) {
                        q->offer(b);
                    }
                }
            });
        }
    }
}
