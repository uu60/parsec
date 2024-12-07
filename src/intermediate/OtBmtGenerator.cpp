//
// Created by 杜建璋 on 2024/8/30.
//

#include "intermediate/OtBmtGenerator.h"

#include <folly/futures/Future.h>

#include "ot/RsaOtExecutor.h"
#include "utils/Math.h"
#include "comm/IComm.h"

OtBmtGenerator::OtBmtGenerator() : AbstractSecureExecutor(64, 0, 0) {
}

OtBmtGenerator &OtBmtGenerator::getInstance() {
    static OtBmtGenerator instance;
    return instance;
}

void OtBmtGenerator::generateRandomAB() {
    _bmt._a = ring(Math::randInt());
    _bmt._b = ring(Math::randInt());
}

void OtBmtGenerator::computeU() {
    computeMix(0, _ui);
}

void OtBmtGenerator::computeV() {
    computeMix(1, _vi);
}

int64_t OtBmtGenerator::corr(int i, int64_t x) const {
    return ring((_bmt._a << i) - x);
}

AbstractSecureExecutor *OtBmtGenerator::reconstruct(int clientRank) {
    return this;
}

void OtBmtGenerator::computeMix(int sender, int64_t &mix) {
    bool isSender = IComm::impl->rank() == sender;
    // atomic integer needed for multiple-thread computation
    std::atomic_int64_t sum = 0;
    std::vector<int8_t> offsets;
    offsets.reserve(_l);

    std::vector<folly::Future<folly::Unit> > futures;
    futures.reserve(_l);

    for (int i = 0; i < _l; i++) {
        offsets.push_back(_currentMsgTag);
        // 6 is number of msgTag needed in RsaOtExecutor
        _currentMsgTag = static_cast<int8_t>(_currentMsgTag + 6);
    }
    for (int i = 0; i < _l; i++) {
        futures.push_back(via(&System::_threadPool, [isSender, this, i, sender, &sum, offsets] {
            int64_t s0 = 0, s1 = 0;
            int choice = 0;
            if (isSender) {
                s0 = ring(Math::randInt(0, 1));
                s1 = corr(i, s0);
            } else {
                choice = static_cast<int>((_bmt._b >> i) & 1);
            }

            RsaOtExecutor r(sender, s0, s1, choice, _l, _objTag, offsets[i]);
            r.execute();

            if (isSender) {
                sum = ring(sum + s0);
            } else {
                int64_t temp = r._result;
                if (choice == 0) {
                    temp = ring(-temp);
                }
                sum = ring(sum + temp);
            }
        }));
    }
    for (auto &f : futures) {
        f.wait();
    }
    mix = sum;
}

void OtBmtGenerator::computeC() {
    _bmt._c = ring(_bmt._a * _bmt._b + _ui + _vi);
}

OtBmtGenerator *OtBmtGenerator::execute() {
    if (IComm::impl->isServer()) {
        generateRandomAB();

        computeU();
        computeV();
        computeC();
    }
    return this;
}

std::string OtBmtGenerator::className() const {
    return "OtBmtGenerator";
}
