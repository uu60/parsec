#include "comm/Comm.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <cstdint>
#include <vector>

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    const int task = System::nextTask();
    const int count = 100;
    const int width = 64;

    BitwiseBmtBatchGenerator gen(count, width, task, 0);
    gen.execute();

    // Verify BMT correctness: a & b = c (XOR with shares from both parties)
    // For verification, we need to reconstruct a, b, c

    // Send shares to rank 0 for verification
    if (Comm::rank() == 1) {
        std::vector<int64_t> as, bs, cs;
        as.reserve(gen._bmts.size());
        bs.reserve(gen._bmts.size());
        cs.reserve(gen._bmts.size());
        for (const auto& bmt : gen._bmts) {
            as.push_back(bmt._a);
            bs.push_back(bmt._b);
            cs.push_back(bmt._c);
        }
        Comm::send(as, 64, 0, task * 1000 + 1);
        Comm::send(bs, 64, 0, task * 1000 + 2);
        Comm::send(cs, 64, 0, task * 1000 + 3);
    } else {
        std::vector<int64_t> as1, bs1, cs1;
        Comm::receive(as1, 64, 1, task * 1000 + 1);
        Comm::receive(bs1, 64, 1, task * 1000 + 2);
        Comm::receive(cs1, 64, 1, task * 1000 + 3);

        int mismatch = 0;
        for (size_t i = 0; i < gen._bmts.size(); ++i) {
            // Reconstruct by XOR
            int64_t a = gen._bmts[i]._a ^ as1[i];
            int64_t b = gen._bmts[i]._b ^ bs1[i];
            int64_t c = gen._bmts[i]._c ^ cs1[i];

            // Check: a & b == c
            int64_t expected = a & b;
            if (expected != c) {
                ++mismatch;
                if (mismatch <= 10) {
                    Log::e("MISMATCH i={}: a={} b={} a&b={} c={}",
                           i, static_cast<uint64_t>(a), static_cast<uint64_t>(b),
                           static_cast<uint64_t>(expected), static_cast<uint64_t>(c));
                }
            }
        }

        Log::i(mismatch == 0 ? "[BitwiseBmt correctness] PASS" : "[BitwiseBmt correctness] FAIL mismatches={}", mismatch);
    }

    System::finalize();
    return 0;
}

