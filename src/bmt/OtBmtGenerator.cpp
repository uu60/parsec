//
// Created by 杜建璋 on 2024/8/30.
//

#include "bmt/OtBmtGenerator.h"
#include "ot/RsaOtExecutor.h"
#include "utils/Math.h"
#include "utils/Comm.h"

OtBmtGenerator::OtBmtGenerator(int l) : SecureExecutor(l) {}

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

void OtBmtGenerator::computeMix(int sender, int64_t &mix) {
    bool isSender = Comm::rank() == sender;
    int64_t sum = 0;
    for (int i = 0; i < _l; i++) {
        int64_t s0 = 0, s1 = 0;
        int choice = 0;
        if (isSender) {
            s0 = ring(Math::randInt(0, 1));
            s1 = corr(i, s0);
        } else {
            choice = static_cast<int>((_bmt._b >> i) & 1);
        }
        RsaOtExecutor r(sender, s0, s1, _l, choice);
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
    }
    mix = sum;
}

void OtBmtGenerator::computeC() {
    _bmt._c = ring(_bmt._a * _bmt._b + _ui + _vi);
}

OtBmtGenerator *OtBmtGenerator::execute() {
    if (Comm::isServer()) {
        generateRandomAB();

        computeU();
        computeV();
        computeC();
    }
    return this;
}

std::string OtBmtGenerator::tag() const {
    return "[BMT Generator]";
}