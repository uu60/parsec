//
// Created by 杜建璋 on 2024/8/30.
//

#include "intermediate/BmtGenerator.h"

#include "ot/RsaOtExecutor.h"
#include "utils/Math.h"
#include "comm/IComm.h"
#include "utils/Log.h"

BmtGenerator::BmtGenerator() : AbstractSecureExecutor(64, 0, 0) {
}

BmtGenerator &BmtGenerator::getInstance() {
    static BmtGenerator instance;
    return instance;
}

void BmtGenerator::generateRandomAB() {
    _bmt._a = Math::randInt();
    _bmt._b = Math::randInt();
}

void BmtGenerator::computeU() {
    computeMix(0, _ui);
}

void BmtGenerator::computeV() {
    computeMix(1, _vi);
}

int64_t BmtGenerator::corr(int i, int64_t x) const {
    return ring((_bmt._a << i) - x);
}

AbstractSecureExecutor *BmtGenerator::reconstruct(int clientRank) {
    return this;
}

void BmtGenerator::computeMix(int sender, int64_t &mix) {
    // atomic integer needed for multiple-thread computation
    std::atomic_int64_t sum = 0;

    std::vector<std::future<void> > futures;
    futures.reserve(_l);

    for (int i = 0; i < _l; i++) {
        futures.push_back(System::_threadPool.push([this, i, sender, &sum](int _) {
            bool isSender = IComm::impl->rank() == sender;
            int64_t s0 = 0, s1 = 0;
            int choice = 0;
            if (isSender) {
                s0 = Math::randInt(0, 1);
                s1 = corr(i, s0);
            } else {
                choice = static_cast<int>((_bmt._b >> i) & 1);
            }

            RsaOtExecutor r(sender, 1 - sender, s0, s1, choice, _l, _objTag,
                            static_cast<int16_t>(_currentMsgTag + i * RsaOtExecutor::neededMsgTags()));
            r.execute();

            if (isSender) {
                sum += s0;
            } else {
                int64_t temp = r._result;
                if (choice == 0) {
                    temp = -temp;
                }
                sum += temp;
            }
        }));
    }

    for (auto &f : futures) {
        f.wait();
    }
    _currentMsgTag = static_cast<int16_t>(_currentMsgTag + RsaOtExecutor::neededMsgTags() * _l);

    mix = sum;
}

void BmtGenerator::computeC() {
    _bmt._c = ring(_bmt._a * _bmt._b + _ui + _vi);
}

BmtGenerator *BmtGenerator::execute() {
    if (IComm::impl->isServer()) {
        generateRandomAB();

        computeU();
        computeV();
        computeC();
    }
    return this;
}

std::string BmtGenerator::className() const {
    return "OtBmtGenerator";
}
