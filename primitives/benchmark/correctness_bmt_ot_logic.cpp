#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "ot/IknpOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <cstdint>
#include <vector>

// Simulates BMT's computeMix logic to test if IKNP OT works correctly for it
void testBmtOtLogic(int sender, int task, int bc) {
    const bool isSender = (Comm::rank() == sender);

    // Simulate BMT's _bmts
    std::vector<int64_t> local_a(bc), local_b(bc);
    for (int i = 0; i < bc; ++i) {
        local_a[i] = Math::randInt();
        local_b[i] = Math::randInt();
    }

    std::vector<int64_t> ss0, ss1;
    std::vector<int64_t> choices;

    if (isSender) {
        ss0.resize(bc);
        ss1.resize(bc);
        for (int i = 0; i < bc; i++) {
            int64_t random = Math::randInt();
            ss0[i] = random;
            ss1[i] = local_a[i] ^ random;  // corr(i, random) = _bmts[i]._a ^ x
        }
    } else {
        choices.resize(bc);
        for (int i = 0; i < bc; i++) {
            choices[i] = local_b[i];
        }
    }

    // Share data for verification BEFORE running OT
    std::vector<int64_t> sender_ss0, sender_ss1;
    if (isSender) {
        // Send ss0, ss1 to receiver for verification
        Comm::send(ss0, 64, 1 - sender, task * 1000 + 1);
        Comm::send(ss1, 64, 1 - sender, task * 1000 + 2);
    } else {
        Comm::receive(sender_ss0, 64, sender, task * 1000 + 1);
        Comm::receive(sender_ss1, 64, sender, task * 1000 + 2);
    }

    // Run OT
    IknpOtBatchOperator op(sender,
                           isSender ? &ss0 : nullptr,
                           isSender ? &ss1 : nullptr,
                           isSender ? nullptr : &choices,
                           task,
                           task * 1000 + 10);
    op.execute();

    if (!isSender) {
        // Verify: for each bit, result should be ss0 if choice=0, ss1 if choice=1
        int mismatch = 0;
        for (int limb = 0; limb < bc; ++limb) {
            for (int bit = 0; bit < 64; ++bit) {
                const uint64_t mask = 1ULL << bit;
                const int c = (choices[limb] & mask) ? 1 : 0;
                const int expected_bit = c ? ((sender_ss1[limb] & mask) ? 1 : 0) : ((sender_ss0[limb] & mask) ? 1 : 0);
                const int got_bit = (op._results[limb] & mask) ? 1 : 0;
                if (expected_bit != got_bit) {
                    ++mismatch;
                    if (mismatch <= 10) {
                        Log::e("sender={} limb={} bit={} choice={} exp={} got={}",
                               sender, limb, bit, c, expected_bit, got_bit);
                    }
                }
            }
        }
        Log::i(mismatch == 0 ? "[BMT OT Logic sender={}] PASS" : "[BMT OT Logic sender={}] FAIL mismatches={}", sender, mismatch);
    }
}

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    const int bc = 100;  // Same as BMT test

    // Test sender=0
    testBmtOtLogic(0, System::nextTask(), bc);

    // Test sender=1
    testBmtOtLogic(1, System::nextTask(), bc);

    System::finalize();
    return 0;
}

