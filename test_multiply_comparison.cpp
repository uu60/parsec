#include <iostream>
#include <vector>
#include "primitives/include/utils/System.h"
#include "primitives/include/utils/Log.h"
#include "primitives/include/comm/Comm.h"
#include "primitives/include/compute/single/arith/ArithMultiplyOperator.h"
#include "primitives/include/compute/batch/arith/ArithMultiplyBatchOperator.h"

int main(int argc, char **argv) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        std::vector<int64_t> a = {10, 20, 30};
        std::vector<int64_t> b = {5, 6, 7};
        int width = 32;
        auto t = System::nextTask();

        Log::i("Testing single vs batch multiplication");
        Log::i("Input a: [{}, {}, {}]", a[0], a[1], a[2]);
        Log::i("Input b: [{}, {}, {}]", b[0], b[1], b[2]);
        Log::i("Expected results: [{}, {}, {}]", a[0]*b[0], a[1]*b[1], a[2]*b[2]);

        // Test batch operation first
        ArithMultiplyBatchOperator batchOp(&a, &b, width, t, 0, 2);
        auto batchResults = batchOp.execute()->reconstruct(2)->_results;
        Log::i("Batch operation results: [{}, {}, {}]", batchResults[0], batchResults[1], batchResults[2]);

        // Test single operations with different task tags
        std::vector<int64_t> singleResults;
        for (int i = 0; i < 3; i++) {
            auto t2 = System::nextTask();
            ArithMultiplyOperator singleOp(a[i], b[i], width, t2, 0, 2);
            auto result = singleOp.execute()->reconstruct(2)->_zi;
            singleResults.push_back(result);
        }
        Log::i("Single operation results: [{}, {}, {}]", singleResults[0], singleResults[1], singleResults[2]);

        // Compare results
        bool allCorrect = true;
        for (int i = 0; i < 3; i++) {
            if (singleResults[i] != batchResults[i]) {
                Log::e("Mismatch at index {}: single={}, batch={}", i, singleResults[i], batchResults[i]);
                allCorrect = false;
            }
        }
        
        if (allCorrect) {
            Log::i("✅ All results match between single and batch operations!");
        } else {
            Log::e("❌ Results do not match between single and batch operations!");
        }
    }

    System::finalize();
    return 0;
}
