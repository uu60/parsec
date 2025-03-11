#include <iostream>

#include "test_cases.h"
#include "cstring"
#include <iostream>
#include <thread>
#include <vector>
#include <string>

#include "../include/utils/Log.h"
#include "../include/utils/System.h"

int main(int argc, char **argv) {
    System::init(argc, argv);
    Log::i("Beginning...");

    std::string caseIdx;
    // try {
    for (int i = 0; i < argc; i++) {
        if (strcmp("-case", argv[i]) == 0) {
            if (i + 1 >= argc) {
                throw std::runtime_error("error");
            }
            caseIdx = argv[i + 1];
        }
    }
    switch (std::stoi(caseIdx)) {
        case 0:
            test_arith_add_0();
            break;
        case 1:
            test_arith_mul_parallel_1();
            break;
        case 2:
            test_bmt_generation_2();
            break;
        case 3:
            test_bitwise_bool_and_3();
            break;
        case 4:
            test_arith_less_4();
            break;
        case 5:
            test_convertion_5();
            break;
        // case 6:
        //     test_bool_MuxExecutor_6();
        //     break;
        case 7:
            test_int_mux_7();
            break;
        // case 8:
        //     test_BoolShareReconstruct_8();
        //     break;
        case 9:
            test_ot_9();
            break;
        case 10:
            test_Sort_10();
            break;
        case 11:
            test_bool_comp_11();
            break;
        case 12:
            test_bool_mux_12();
            break;
        case 14:
            test_api_14();
            break;
        case 15:
            test_batch_and_15();
            break;
        case 16:
            test_batch_bool_mux_16();
            break;
    }
    // }
    /*catch (...) {
        Log::e("Wrong argument");
        System::finalize();
        return 0;
    }*/

    Log::i("Done.");
    System::finalize();
    return 0;
}
