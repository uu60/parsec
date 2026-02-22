#include "intermediate/IntermediateDataSupport.h"

#include "comm/Comm.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/BitwiseBmtGenerator.h"
#include "intermediate/BmtGenerator.h"
#include "ot/BaseOtOperator.h"
#include "parallel/ThreadPoolSupport.h"
#include "sync/BoostLockFreeQueue.h"
#include "sync/BoostSpscQueue.h"
#include "sync/LockBlockingQueue.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include <climits>
#include <stdexcept>

#include "intermediate/PipelineBitwiseBmtBatchGenerator.h"
#include "ot/BaseOtBatchOperator.h"
#include "utils/Crypto.h"

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

    if ((Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) && Conf::BMT_PRE_GEN_SECONDS > 0) {
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
            _sRot0->_r0 = Math::randInt();
            _sRot0->_r1 = Math::randInt();
        } else {
            _rRot0 = new RRot();
            _rRot0->_b = static_cast<int>(Math::randInt(0, 1));
        }
        BaseOtOperator e(i, isSender ? _sRot0->_r0 : -1, isSender ? _sRot0->_r1 : -1, !isSender ? _rRot0->_b : -1,
                         64, 2, i * BaseOtOperator::tagStride());
        e.execute();
        if (!isSender) {
            _rRot0->_rb = e._result;
        }
    }

    for (int i = 0; i < 2; i++) {
        bool isSender = Comm::rank() == i;
        if (isSender) {
            _sRot1 = new SRot();
            _sRot1->_r0 = Math::randInt();
            _sRot1->_r1 = Math::randInt();
        } else {
            _rRot1 = new RRot();
            _rRot1->_b = 1 - _rRot0->_b;
        }
        BaseOtOperator e(i, isSender ? _sRot1->_r0 : -1, isSender ? _sRot1->_r1 : -1, !isSender ? _rRot1->_b : -1,
                         64, 2, i * BaseOtOperator::tagStride());
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
                } catch (...) {}
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
                        auto bitwiseBmts = BitwiseBmtBatchGenerator(Conf::BMT_GEN_BATCH_SIZE, 64, i, 0).execute()->_bmts;
                        for (auto b: bitwiseBmts) {
                            q->offer(b);
                        }
                    }
                } catch (...) {}
            });
        }
    } else if (Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
        for (int i = 0; i < Conf::BMT_QUEUE_NUM; i++) {
            ThreadPoolSupport::submit([i] {
                try {
                    auto g = new PipelineBitwiseBmtBatchGenerator(i, i, 0);
                    g->execute();
                    delete g;
                } catch (...) {}
            });
        }
    }
}

void IntermediateDataSupport::prepareIknp() {
    if (Comm::isClient()) {
        return;
    }

    if (!_iknpBaseSeeds0.empty()) {
        return;  // Already initialized
    }

    // Prepare IKNP base data for BOTH directions:
    // Direction 0: sender=0 (rank 0 is IKNP Sender, rank 1 is IKNP Receiver)
    // Direction 1: sender=1 (rank 1 is IKNP Sender, rank 0 is IKNP Receiver)
    //
    // In IKNP, roles are REVERSED from base OT:
    // - IKNP Sender acts as Base OT Receiver (gets one seed per choice bit)
    // - IKNP Receiver acts as Base OT Sender (provides two seeds)

    _iknpBaseSeeds0.resize(128);
    _iknpBaseSeeds1.resize(128);
    _iknpSenderChoices0.resize(128, false);
    _iknpSenderChoices1.resize(128, false);

    const int taskTag = 1;

    // ========== Direction 0: sender=0 ==========
    // IKNP Sender = rank 0 = Base OT Receiver
    // IKNP Receiver = rank 1 = Base OT Sender
    {
        const int baseOtSender = 1;
        const int msgTagOffset = 100;

        std::vector<int64_t> ms0, ms1;
        std::vector<int> choices;

        const bool isBaseOtSender = (Comm::rank() == baseOtSender);

        if (isBaseOtSender) {
            // rank 1: Base OT sender, generate seed pairs
            ms0.resize(128);
            ms1.resize(128);
            for (int i = 0; i < 128; ++i) {
                ms0[i] = Math::randInt();
                ms1[i] = Math::randInt();
            }
        } else {
            // rank 0: Base OT receiver, generate choice bits
            choices.resize(128);
            for (int i = 0; i < 128; ++i) {
                choices[i] = static_cast<int>(Math::randInt(0, 1));
            }
        }

        BaseOtBatchOperator baseOt(baseOtSender, &ms0, &ms1, &choices, 64, taskTag, msgTagOffset);
        baseOt.execute();

        if (isBaseOtSender) {
            // rank 1 is IKNP Receiver for direction 0, stores both seeds
            for (int i = 0; i < 128; ++i) {
                _iknpBaseSeeds0[i] = {ms0[i], ms1[i]};
            }
        } else {
            // rank 0 is IKNP Sender for direction 0, stores chosen seed
            for (int i = 0; i < 128; ++i) {
                _iknpBaseSeeds0[i] = {baseOt._results[i], baseOt._results[i]};
                _iknpSenderChoices0[i] = (choices[i] != 0);
            }
        }

        // Sync choice bits: IKNP Sender (rank 0) sends to IKNP Receiver (rank 1)
        const int syncTag = 200;
        if (!isBaseOtSender) {
            std::vector<int64_t> choiceBits(128);
            for (int i = 0; i < 128; ++i) {
                choiceBits[i] = _iknpSenderChoices0[i] ? 1 : 0;
            }
            Comm::serverSend(choiceBits, 1, syncTag);
        } else {
            std::vector<int64_t> choiceBits;
            Comm::serverReceive(choiceBits, 1, syncTag);
            for (int i = 0; i < 128; ++i) {
                _iknpSenderChoices0[i] = (choiceBits[i] & 1) != 0;
            }
        }
    }

    // ========== Direction 1: sender=1 ==========
    // IKNP Sender = rank 1 = Base OT Receiver
    // IKNP Receiver = rank 0 = Base OT Sender
    {
        const int baseOtSender = 0;
        const int msgTagOffset = 300;

        std::vector<int64_t> ms0, ms1;
        std::vector<int> choices;

        const bool isBaseOtSender = (Comm::rank() == baseOtSender);

        if (isBaseOtSender) {
            // rank 0: Base OT sender, generate seed pairs
            ms0.resize(128);
            ms1.resize(128);
            for (int i = 0; i < 128; ++i) {
                ms0[i] = Math::randInt();
                ms1[i] = Math::randInt();
            }
        } else {
            // rank 1: Base OT receiver, generate choice bits
            choices.resize(128);
            for (int i = 0; i < 128; ++i) {
                choices[i] = static_cast<int>(Math::randInt(0, 1));
            }
        }

        BaseOtBatchOperator baseOt(baseOtSender, &ms0, &ms1, &choices, 64, taskTag, msgTagOffset);
        baseOt.execute();

        if (isBaseOtSender) {
            // rank 0 is IKNP Receiver for direction 1, stores both seeds
            for (int i = 0; i < 128; ++i) {
                _iknpBaseSeeds1[i] = {ms0[i], ms1[i]};
            }
        } else {
            // rank 1 is IKNP Sender for direction 1, stores chosen seed
            for (int i = 0; i < 128; ++i) {
                _iknpBaseSeeds1[i] = {baseOt._results[i], baseOt._results[i]};
                _iknpSenderChoices1[i] = (choices[i] != 0);
            }
        }

        // Sync choice bits: IKNP Sender (rank 1) sends to IKNP Receiver (rank 0)
        const int syncTag = 400;
        if (!isBaseOtSender) {
            std::vector<int64_t> choiceBits(128);
            for (int i = 0; i < 128; ++i) {
                choiceBits[i] = _iknpSenderChoices1[i] ? 1 : 0;
            }
            Comm::serverSend(choiceBits, 1, syncTag);
        } else {
            std::vector<int64_t> choiceBits;
            Comm::serverReceive(choiceBits, 1, syncTag);
            for (int i = 0; i < 128; ++i) {
                _iknpSenderChoices1[i] = (choiceBits[i] & 1) != 0;
            }
        }
    }
}