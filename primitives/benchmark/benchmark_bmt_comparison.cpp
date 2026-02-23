#include "comm/Comm.h"
#include "intermediate/BmtBatchGenerator.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "intermediate/IntermediateDataSupport.h"
#include "utils/Log.h"
#include "utils/System.h"

#include <cstdint>
#include <vector>

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    const int count = 100;
    const int width = 64;
    const int iterations = 5;

    // Warm up
    {
        int task = System::nextTask();
        BmtBatchGenerator gen(count, width, task, 0);
        gen.execute();
    }
    {
        int task = System::nextTask();
        BitwiseBmtBatchGenerator gen(count, width, task, 0);
        gen.execute();
    }

    // Benchmark BmtBatchGenerator (scalar OT)
    int64_t scalarTotal = 0;
    for (int i = 0; i < iterations; ++i) {
        int task = System::nextTask();
        int64_t start = System::currentTimeMillis();
        BmtBatchGenerator gen(count, width, task, 0);
        gen.execute();
        int64_t elapsed = System::currentTimeMillis() - start;
        scalarTotal += elapsed;
        if (Comm::rank() == 0) {
            Log::i("[Scalar BMT] iter={} time={}ms", i, elapsed);
        }
    }

    // Benchmark BitwiseBmtBatchGenerator (packed-bits OT)
    int64_t bitwiseTotal = 0;
    for (int i = 0; i < iterations; ++i) {
        int task = System::nextTask();
        int64_t start = System::currentTimeMillis();
        BitwiseBmtBatchGenerator gen(count, width, task, 0);
        gen.execute();
        int64_t elapsed = System::currentTimeMillis() - start;
        bitwiseTotal += elapsed;
        if (Comm::rank() == 0) {
            Log::i("[Bitwise BMT] iter={} time={}ms", i, elapsed);
        }
    }

    if (Comm::rank() == 0) {
        Log::i("=== Summary (count={}, width={}, iterations={}) ===", count, width, iterations);
        Log::i("Scalar BMT:  avg={}ms total={}ms", scalarTotal / iterations, scalarTotal);
        Log::i("Bitwise BMT: avg={}ms total={}ms", bitwiseTotal / iterations, bitwiseTotal);
        Log::i("Speedup: {:.2f}x", static_cast<double>(scalarTotal) / static_cast<double>(bitwiseTotal));
    }

    System::finalize();
    return 0;
}

