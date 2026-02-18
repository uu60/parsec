//
// Created by 杜建璋 on 26-1-17.
//

#include "ot/BaseOtBatchOperator.h"
#include "ot/BaseOtOperator.h"
#include "ot/RandOtBatchOperator.h"
#include "utils/Log.h"
#include "utils/Math.h"
#include "utils/StringUtils.h"
#include "utils/System.h"

int main(int argc, char *argv[]) {
    System::init(argc, argv);

    if (Comm::isClient()) {
        System::finalize();
        return 0;
    }

    bool isSender = Comm::rank() == 0;

    std::vector<int64_t> ss0, ss1;
    std::vector<int> choices;

    if (isSender) {
        ss0.resize(10);
        ss1.resize(10);
        for (int i = 0; i < 10; i++) {
            int64_t random = i;
            ss0[i] = random;
            ss1[i] = i + 100;
        }
    } else {
        choices.resize(10);
        for (int i = 0; i < 10; i++) {
            choices[i] = 0;
        }
    }

    auto results = BaseOtBatchOperator(0, &ss0, &ss1, &choices, 64, 1, 0).execute()->_results;

    // std::vector<int64_t> results;
    // for (int i = 0; i < 10; i++) {
    //     results.push_back(BaseOtOperator(0, isSender ? ss0[i] : -1, isSender ? ss1[i] : -1, !isSender ? choices[i] : -1, 64, 1, 0).execute()->_result);
    // }

    if (Comm::rank() == 1) {
        Log::i("{}", StringUtils::vecToString(results));
    }

    System::finalize();
}
