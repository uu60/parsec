#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "conf/Conf.h"
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
    auto bmts = IntermediateDataSupport::pollBmts(count, 64);

    if (Comm::rank() == 1) {
        std::vector<int64_t> as, bs, cs;
        for (const auto &bmt: bmts) {
            as.push_back(bmt._a);
            bs.push_back(bmt._b);
            cs.push_back(bmt._c);
        }
        Comm::send(as, 64, 0, task * 1000 + 1);
        Comm::send(bs, 64, 0, task * 1000 + 2);
        Comm::send(cs, 64, 0, task * 1000 + 3);
    } else {
        std::vector<int64_t> as, bs, cs;
        Comm::receive(as, 64, 1, task * 1000 + 1);
        Comm::receive(bs, 64, 1, task * 1000 + 2);
        Comm::receive(cs, 64, 1, task * 1000 + 3);
        int mismatches = 0;
        for (int i = 0; i < count; ++i) {
            const uint64_t a = static_cast<uint64_t>(bmts[i]._a) + static_cast<uint64_t>(as[i]);
            const uint64_t b = static_cast<uint64_t>(bmts[i]._b) + static_cast<uint64_t>(bs[i]);
            const uint64_t c = static_cast<uint64_t>(bmts[i]._c) + static_cast<uint64_t>(cs[i]);
            if (a * b != c) ++mismatches;
        }
        if (mismatches == 0) {
            Log::i("[Arithmetic background BMT correctness] PASS");
        } else {
            Log::e("[Arithmetic background BMT correctness] FAIL mismatches={}", mismatches);
            System::finalize();
            return 1;
        }
    }

    System::finalize();
    return 0;
}
