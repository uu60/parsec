#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "ot/IknpOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <algorithm>
#include <cstdint>
#include <vector>

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    IntermediateDataSupport::init();

    const int task = System::nextTask();
    const int sender = 0;
    const int width = 32;
    const int n = 2048;

    const bool isSender = (Comm::rank() == sender);

    std::vector<int64_t> ms0, ms1;
    std::vector<int> choices;

    if (isSender) {
        ms0.resize(n);
        ms1.resize(n);
        for (int i = 0; i < n; ++i) {
            ms0[i] = Math::ring(Math::randInt(), width);
            ms1[i] = Math::ring(Math::randInt(), width);
        }
    } else {
        choices.resize(n);
        for (int i = 0; i < n; ++i) {
            choices[i] = static_cast<int>(Math::randInt(0, 1));
        }
    }

    // Send choices to sender for verification.
    if (!isSender) {
        std::vector<int64_t> c64(n);
        for (int i = 0; i < n; ++i) c64[i] = choices[i];
        Comm::send(c64, 1, sender, task * 1000 + 1);
    } else {
        std::vector<int64_t> c64;
        Comm::receive(c64, 1, 1 - sender, task * 1000 + 1);
        choices.resize(n);
        for (int i = 0; i < n; ++i) choices[i] = static_cast<int>(c64[i]);
    }

    IknpOtBatchOperator op(sender,
                           isSender ? &ms0 : nullptr,
                           isSender ? &ms1 : nullptr,
                           isSender ? nullptr : &choices,
                           width,
                           task,
                           task * 1000 + 10);
    op.execute();

    if (!isSender) {
        Comm::send(op._results, width, sender, task * 1000 + 2);
    } else {
        std::vector<int64_t> recv;
        Comm::receive(recv, width, 1 - sender, task * 1000 + 2);

        int mism = 0;
        for (int i = 0; i < n; ++i) {
            const int c = choices[i];
            const int64_t exp = c == 0 ? ms0[i] : ms1[i];
            const int64_t got = Math::ring(recv[i], width);
            if (got != exp) {
                ++mism;
                if (mism <= 10) {
                    Log::e("MISMATCH i={} c={} exp={} got={}", i, c, exp, got);
                }
            }
        }

        Log::i(mism == 0 ? "[IKNP OT correctness] PASS" : "[IKNP OT correctness] FAIL mismatches={}", mism);
    }

    System::finalize();
    return 0;
}

