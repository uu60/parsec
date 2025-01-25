//
// Created by 杜建璋 on 2024/8/30.
//

#include "intermediate/BmtGenerator.h"

#include "utils/Math.h"
#include "comm/Comm.h"
#include "ot/RandOtExecutor.h"
#include "utils/Log.h"

void BmtGenerator::generateRandomAB() {
    _bmt._a = ring(Math::randInt());
    _bmt._b = ring(Math::randInt());
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

BmtGenerator *BmtGenerator::reconstruct(int clientRank) {
    return this;
}

int16_t BmtGenerator::needMsgTags(int l) {
    return l * RandOtExecutor::needMsgTags();
}

void BmtGenerator::computeMix(int sender, int64_t &mix) {
    // atomic integer needed for multiple-thread computation
    std::atomic_int64_t sum = 0;

    std::vector<std::future<void> > futures;
    futures.reserve(_l);

    for (int i = 0; i < _l; i++) {
        // futures.push_back(System::_threadPool.push([this, i, sender, &sum](int _) {
        //     bool isSender = Comm::rank() == sender;
        //     int64_t s0 = 0, s1 = 0;
        //     int choice = 0;
        //     if (isSender) {
        //         s0 = Math::randInt(0, 1);
        //         s1 = corr(i, s0);
        //     } else {
        //         choice = static_cast<int>((_bmt._b >> i) & 1);
        //     }
        //
        //     RandOtExecutor r(sender, s0, s1, choice, _l, _taskTag,
        //                      static_cast<int16_t>(
        //                          _currentMsgTag + i * RandOtExecutor::needMsgTags()));
        //     r.execute();
        //
        //     if (isSender) {
        //         sum += s0;
        //     } else {
        //         int64_t temp = r._result;
        //         if (choice == 0) {
        //             temp = -temp;
        //         }
        //         sum += temp;
        //     }
        // }));
        bool isSender = Comm::rank() == sender;
        int64_t s0 = 0, s1 = 0;
        int choice = 0;
        if (isSender) {
            s0 = Math::randInt(0, 1);
            s1 = corr(i, s0);
        } else {
            choice = static_cast<int>((_bmt._b >> i) & 1);
        }

        RandOtExecutor r(sender, s0, s1, choice, _l, _taskTag,
                         static_cast<int16_t>(
                             _currentMsgTag + i * RandOtExecutor::needMsgTags()));
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
    }

    for (auto &f: futures) {
        f.wait();
    }

    mix = ring(sum);
}

void BmtGenerator::computeC() {
    _bmt._c = ring(_bmt._a * _bmt._b + _ui + _vi);
}

BmtGenerator *BmtGenerator::execute() {
    _currentMsgTag = _startMsgTag;
    if (Comm::isServer()) {
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
