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
    const int count = Conf::_userParams.count("count")
                          ? std::stoi(Conf::_userParams["count"])
                          : 100;
    const int width = 64;

    std::vector<BitwiseBmt> bmts;
    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND || Conf::BMT_METHOD == Conf::BMT_PIPELINE) {
        if (Conf::_userParams.count("chunked") && Conf::_userParams["chunked"] == "true") {
            int remaining = count;
            int chunk = 1;
            while (remaining > 0) {
                const int take = std::min(remaining, chunk);
                auto part = IntermediateDataSupport::pollBitwiseBmts(take, width);
                bmts.insert(bmts.end(), part.begin(), part.end());
                remaining -= take;
                chunk = chunk % 17 + 1;
            }
        } else {
            bmts = IntermediateDataSupport::pollBitwiseBmts(count, width);
        }
    } else {
        BitwiseBmtBatchGenerator gen(count, width, task, 0);
        gen.execute();
        bmts = std::move(gen._bmts);
    }

    // Verify BMT correctness: a & b = c (XOR with shares from both parties)
    // For verification, we need to reconstruct a, b, c

    // Send shares to rank 0 for verification
    if (Comm::rank() == 1) {
        std::vector<int64_t> as, bs, cs;
        as.reserve(bmts.size());
        bs.reserve(bmts.size());
        cs.reserve(bmts.size());
        for (const auto& bmt : bmts) {
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
        for (size_t i = 0; i < bmts.size(); ++i) {
            // Reconstruct by XOR
            int64_t a = bmts[i]._a ^ as1[i];
            int64_t b = bmts[i]._b ^ bs1[i];
            int64_t c = bmts[i]._c ^ cs1[i];

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
