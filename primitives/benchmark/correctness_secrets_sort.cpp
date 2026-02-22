#include "comm/Comm.h"
#include "intermediate/IntermediateDataSupport.h"
#include "secret/Secrets.h"
#include "secret/item/ArithSecret.h"
#include "secret/item/BoolSecret.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/System.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

int main(int argc, char **argv) {
    System::init(argc, argv);

    // IntermediateDataSupport::init();

    const int task = System::nextTask();
    const int width = 32;
    const int n = 100;

    // Parse mode from command line (default: arith)
    std::string mode = "arith";
    if (argc > 1) {
        mode = argv[1];
        if (mode != "arith" && mode != "bool") {
            Log::e("Invalid mode '{}'. Use 'arith' or 'bool'", mode);
            System::finalize();
            return 1;
        }
    }

    // Generate original data on client
    std::vector<int64_t> origins;
    if (Comm::isClient()) {
        origins.resize(n);
        for (int i = 0; i < n; i++) {
            origins[i] = Math::randInt(0, 1000);
        }
        if (mode == "arith") {
            Secrets::arithShare(origins, 2, width, task);
            std::vector<int64_t> result = Secrets::arithReconstruct(origins, 2, width, task);

            // Sort the original data for comparison
            std::sort(origins.begin(), origins.end());

            // Verify correctness
            int mismatch = 0;
            for (int i = 0; i < n; i++) {
                int64_t expected = Math::ring(origins[i], width);
                int64_t got = Math::ring(result[i], width);
                if (expected != got) {
                    mismatch++;
                    if (mismatch <= 10) {
                        Log::e("MISMATCH at index {}: expected={}, got={}", i, expected, got);
                    }
                }
            }

            if (mismatch == 0) {
                Log::i("[ArithSecret Sort correctness] PASS");
            } else {
                Log::i("[ArithSecret Sort correctness] FAIL mismatches={}", mismatch);
            }
        } else {
            Secrets::boolShare(origins, 2, width, task);
            std::vector<int64_t> result = Secrets::boolReconstruct(origins, 2, width, task);

            // Sort the original data for comparison
            std::sort(origins.begin(), origins.end());

            // Verify correctness
            int mismatch = 0;
            for (int i = 0; i < n; i++) {
                int64_t expected = Math::ring(origins[i], width);
                int64_t got = Math::ring(result[i], width);
                if (expected != got) {
                    mismatch++;
                    if (mismatch <= 10) {
                        Log::e("MISMATCH at index {}: expected={}, got={}", i, expected, got);
                    }
                }
            }

            if (mismatch == 0) {
                Log::i("[BoolSecret Sort correctness] PASS");
            } else {
                Log::i("[BoolSecret Sort correctness] FAIL mismatches={}", mismatch);
            }
        }
    } else {
        if (mode == "arith") {
            // ArithSecret Sort Test
            std::vector<int64_t> secrets = Secrets::arithShare(origins, 2, width, task);

            if (Comm::isServer()) {
                // Create ArithSecret objects
                std::vector<ArithSecret> arithSecrets;
                arithSecrets.reserve(n);
                for (int i = 0; i < n; i++) {
                    arithSecrets.emplace_back(secrets[i], width, task, 0);
                }

                // Perform sort (ascending)
                Secrets::sort(arithSecrets, true, task);

                // Extract sorted shares
                std::vector<int64_t> sorted;
                sorted.reserve(n);
                for (int i = 0; i < n; i++) {
                    sorted.push_back(arithSecrets[i]._data);
                }

                // Reconstruct to client for verification
                std::vector<int64_t> result = Secrets::arithReconstruct(sorted, 2, width, task);
            }
        } else {
            // BoolSecret Sort Test
            std::vector<int64_t> secrets = Secrets::boolShare(origins, 2, width, task);

            if (Comm::isServer()) {
                // Create BoolSecret objects
                std::vector<BoolSecret> boolSecrets;
                boolSecrets.reserve(n);
                for (int i = 0; i < n; i++) {
                    boolSecrets.emplace_back(secrets[i], width, task, 0);
                }

                // Perform sort (ascending)
                Secrets::sort(boolSecrets, true, task);

                // Extract sorted shares
                std::vector<int64_t> sorted;
                sorted.reserve(n);
                for (int i = 0; i < n; i++) {
                    sorted.push_back(boolSecrets[i]._data);
                }

                // Reconstruct to client for verification
                std::vector<int64_t> result = Secrets::boolReconstruct(sorted, 2, width, task);
            }
        }
    }

    System::finalize();
    return 0;
}
