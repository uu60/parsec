#include "parsecdb/intermediate/IntermediateDataSupport.h"

#include "parsecdb/comm/Comm.h"
#include "parsecdb/intermediate/BitwiseBmtBatchGenerator.h"
#include "parsecdb/intermediate/BitwiseBmtGenerator.h"
#include "parsecdb/intermediate/BmtGenerator.h"
#include "parsecdb/ot/BaseOtOperator.h"
#include "parsecdb/parallel/ThreadPoolSupport.h"
#include "parsecdb/sync/BoostLockFreeQueue.h"
#include "parsecdb/sync/BoostSpscQueue.h"
#include "parsecdb/sync/LockBlockingQueue.h"
#include "parsecdb/utils/Log.h"
#include "parsecdb/utils/Math.h"
#include <climits>
#include <stdexcept>

#include "parsecdb/intermediate/PipelineBitwiseBmtBatchGenerator.h"
#include "parsecdb/ot/BaseOtBatchOperator.h"
#include "parsecdb/utils/Crypto.h"

namespace parsecdb {

void IntermediateDataSupport::prepareBmt() {
    if (Conf::BMT_METHOD == Conf::BMT_FIXED) {
        _fixedBmt = BmtGenerator(64, 0, 0).execute()->_bmt;
        _fixedBitwiseBmt = BitwiseBmtGenerator(64, 0, 0).execute()->_bmt;
    } else if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
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
                _bitwiseBmtQs[i] = new BoostSPSCQueue<BitwiseBmt, 10000000>();
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
                _bmtQs[i] = new BoostSPSCQueue<Bmt, 10000000>();
            }
        }
        startGenerateBmtsAsync();
    }
}

void IntermediateDataSupport::init() {
    if (Comm::isClient()) {
        return;
    }

    prepareBaseOtRsaKeys();

    prepareRot();

    prepareIknp();

    prepareBmt();

    if ((Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) &&
        Conf::BMT_PRE_GEN_SECONDS > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(Conf::BMT_PRE_GEN_SECONDS));
    }
}

void IntermediateDataSupport::finalize() {
    delete _currentBmt;
    delete _currentBitwiseBmt;
    delete _sRot0;
    delete _rRot0;
    delete _sRot1;
    delete _rRot1;
}

void IntermediateDataSupport::prepareBaseOtRsaKeys() {
    if (Comm::isClient()) {
        return;
    }

    const int bits = 2048;
    const int tag = 1;

    // Each rank generates its own RSA key pair
    for (int i = 0; i < 2; i++) {
        if (Comm::rank() == i) {
            // Generate my RSA key pair
            Crypto::generateRsaKeys(bits);
            _baseOtSelfPub = Crypto::_selfPubs[bits];
            _baseOtSelfPri = Crypto::_selfPris[bits];

            // Send public key to the other party
            Comm::serverSend(_baseOtSelfPub, tag);
        } else {
            // Receive the other party's public key
            Comm::serverReceive(_baseOtOtherPub, tag);
        }
    }
}

void IntermediateDataSupport::prepareRot() {
    if (Comm::isClient()) {
        return;
    }

    for (int i = 0; i < 2; i++) {
        bool isSender = Comm::rank() == i;
        if (isSender) {
            _sRot0 = new SRot();
            _sRot0->_r0 = Math::randBits();
            _sRot0->_r1 = Math::randBits();
        } else {
            _rRot0 = new RRot();
            _rRot0->_b = static_cast<int>(Math::randInt(0, 1));
        }
        BaseOtOperator e(i, isSender ? _sRot0->_r0 : -1, isSender ? _sRot0->_r1 : -1, !isSender ? _rRot0->_b : -1, 64,
                         2, i * BaseOtOperator::tagStride());
        e.execute();
        if (!isSender) {
            _rRot0->_rb = e._result;
        }
    }

    for (int i = 0; i < 2; i++) {
        bool isSender = Comm::rank() == i;
        if (isSender) {
            _sRot1 = new SRot();
            _sRot1->_r0 = Math::randBits();
            _sRot1->_r1 = Math::randBits();
        } else {
            _rRot1 = new RRot();
            _rRot1->_b = 1 - _rRot0->_b;
        }
        BaseOtOperator e(i, isSender ? _sRot1->_r0 : -1, isSender ? _sRot1->_r1 : -1, !isSender ? _rRot1->_b : -1, 64,
                         2, i * BaseOtOperator::tagStride());
        e.execute();
        if (!isSender) {
            _rRot1->_rb = e._result;
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
                try {
                    auto q = _bmtQs[i];
                    while (!System::_shutdown.load()) {
                        q->offer(BmtGenerator(64, Conf::BMT_QUEUE_NUM + i, 0).execute()->_bmt);
                    }
                } catch (...) {
                }
            });
        }
    }
}

void IntermediateDataSupport::startGenerateBitwiseBmtsAsync() {
    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
        for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
            ThreadPoolSupport::submit([i] {
                try {
                    auto q = _bitwiseBmtQs[i];
                    while (!System::_shutdown.load()) {
                        auto bitwiseBmts =
                            BitwiseBmtBatchGenerator(Conf::BMT_GEN_BATCH_SIZE, 64, i, 0).execute()->_bmts;
                        for (auto b : bitwiseBmts) {
                            q->offer(b);
                        }
                    }
                } catch (...) {
                }
            });
        }
    } else if (Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
        for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
            ThreadPoolSupport::submit([i] {
                try {
                    auto g = new PipelineBitwiseBmtBatchGenerator(i, i, 0);
                    g->execute();
                    delete g;
                } catch (...) {
                }
            });
        }
    }
}

void IntermediateDataSupport::prepareIknp() {
    if (Comm::isClient()) {
        return;
    }
    if (!_iknpBaseSeeds0.empty()) {
        return; // Already initialized
    }
    // Direction 0: rank 0 is the IKNP sender. Direction 1: rank 1 is the IKNP sender.
    prepareIknpDirection(0, 100, _iknpBaseSeeds0, _iknpSenderChoices0);
    prepareIknpDirection(1, 300, _iknpBaseSeeds1, _iknpSenderChoices1);
}

void IntermediateDataSupport::prepareIknpDirection(int iknpSender, int msgTagOffset,
                                                   std::vector<std::array<U128, 2>>& seeds,
                                                   std::vector<bool>& senderChoices) {
    // IKNP reverses the base-OT roles: the IKNP receiver is the base-OT sender and supplies two seeds per row; the
    // IKNP sender is the base-OT receiver, picks one seed per row with its secret choice bit s_i, and keeps s secret.
    //
    // Each 128-bit seed is one base OT in 128-bit message mode (low and high words under one RSA exchange).
    constexpr int kRows = 128;
    const int baseOtSender = 1 - iknpSender;
    const bool isBaseOtSender = Comm::rank() == baseOtSender;
    const int taskTag = 1;

    std::vector<int64_t> lo0, lo1, hi0, hi1;
    std::vector<int> choices;
    if (isBaseOtSender) {
        lo0.resize(kRows);
        lo1.resize(kRows);
        hi0.resize(kRows);
        hi1.resize(kRows);
        for (int i = 0; i < kRows; ++i) {
            lo0[i] = Math::randBits();
            hi0[i] = Math::randBits();
            lo1[i] = Math::randBits();
            hi1[i] = Math::randBits();
        }
    } else {
        choices.resize(kRows);
        for (int i = 0; i < kRows; ++i) {
            choices[i] = static_cast<int>(Math::randInt(0, 1));
        }
    }

    BaseOtBatchOperator baseOt(baseOtSender, &lo0, &lo1, &choices, 64, taskTag, msgTagOffset);
    baseOt.withHighWords(isBaseOtSender ? &hi0 : nullptr, isBaseOtSender ? &hi1 : nullptr)->execute();

    seeds.assign(kRows, {});
    if (isBaseOtSender) {
        // IKNP receiver: keeps both seeds of every row.
        for (int i = 0; i < kRows; ++i) {
            seeds[i][0] = {static_cast<uint64_t>(lo0[i]), static_cast<uint64_t>(hi0[i])};
            seeds[i][1] = {static_cast<uint64_t>(lo1[i]), static_cast<uint64_t>(hi1[i])};
        }
    } else {
        // IKNP sender: keeps the chosen seed k_i^{s_i} and the choice bits.
        senderChoices.assign(kRows, false);
        for (int i = 0; i < kRows; ++i) {
            seeds[i][0] = {static_cast<uint64_t>(baseOt._results[i]), static_cast<uint64_t>(baseOt._resultsHi[i])};
            seeds[i][1] = seeds[i][0];
            senderChoices[i] = choices[i] != 0;
        }
    }
}

} // namespace parsecdb
