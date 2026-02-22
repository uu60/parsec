#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "ot/IknpOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <cstdint>
#include <vector>

void testDirection(int sender, int task, int limbs) {
    const bool isSender = (Comm::rank() == sender);

    std::vector<int64_t> bits0Packed;
    std::vector<int64_t> bits1Packed;
    std::vector<int64_t> choiceBitsPacked;

    if (isSender) {
        bits0Packed.resize(limbs);
        bits1Packed.resize(limbs);
        for (int i = 0; i < limbs; ++i) {
            bits0Packed[i] = Math::randInt();
            bits1Packed[i] = Math::randInt();
        }
    } else {
        choiceBitsPacked.resize(limbs);
        for (int i = 0; i < limbs; ++i) {
            choiceBitsPacked[i] = Math::randInt();
        }
    }

    // Send packed choices to sender for verification.
    if (!isSender) {
        Comm::send(choiceBitsPacked, 64, sender, task * 1000 + 1);
    } else {
        Comm::receive(choiceBitsPacked, 64, 1 - sender, task * 1000 + 1);
    }

    IknpOtBatchOperator op(sender,
                           isSender ? &bits0Packed : nullptr,
                           isSender ? &bits1Packed : nullptr,
                           isSender ? nullptr : &choiceBitsPacked,
                           task,
                           task * 1000 + 10);
    op.execute();

    if (!isSender) {
        Comm::send(op._results, 64, sender, task * 1000 + 2);
    } else {
        std::vector<int64_t> recv;
        Comm::receive(recv, 64, 1 - sender, task * 1000 + 2);

        int mism = 0;
        for (int limb = 0; limb < limbs; ++limb) {
            const uint64_t c = static_cast<uint64_t>(choiceBitsPacked[limb]);
            const uint64_t m0 = static_cast<uint64_t>(bits0Packed[limb]);
            const uint64_t m1 = static_cast<uint64_t>(bits1Packed[limb]);
            const uint64_t got = static_cast<uint64_t>(recv[limb]);

            for (int bit = 0; bit < 64; ++bit) {
                const uint64_t mask = 1ULL << bit;
                const int expBit = (c & mask) ? ((m1 & mask) ? 1 : 0) : ((m0 & mask) ? 1 : 0);
                const int gotBit = (got & mask) ? 1 : 0;
                if (expBit != gotBit) {
                    ++mism;
                    if (mism <= 10) {
                        Log::e("sender={} MISMATCH limb={} bit={} exp={} got={}", sender, limb, bit, expBit, gotBit);
                    }
                }
            }
        }

        Log::i(mism == 0 ? "[IKNP OT forbits sender={}] PASS" : "[IKNP OT forbits sender={}] FAIL mismatches={}", sender, mism);
    }
}

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    const int limbs = 256; // 256 limbs * 64 bits = 16384 OTs

    // Test sender=0
    testDirection(0, System::nextTask(), limbs);

    // Test sender=1
    testDirection(1, System::nextTask(), limbs);

    System::finalize();
    return 0;
}

