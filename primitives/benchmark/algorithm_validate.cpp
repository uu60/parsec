#include <iostream>

#include "algorithm_validate.h"
#include "cstring"
#include <iostream>
#include <thread>
#include <vector>
#include <string>

#include "../include/utils/Log.h"
#include "../include/utils/System.h"

int main(int argc, char **argv) {
    System::init(argc, argv);

    std::string caseIdx;
    int num = 100, width = 64;
    // try {
    for (int i = 0; i < argc; i++) {
        if (strcmp("--case", argv[i]) == 0) {
            if (i + 1 >= argc) {
                throw std::runtime_error("error");
            }
            caseIdx = argv[i + 1];
        }
        if (strcmp("--num", argv[i]) == 0) {
            if (i + 1 >= argc) {
                throw std::runtime_error("error");
            }
            num = std::stoi(argv[i + 1]);
        }
        if (strcmp("--width", argv[i]) == 0) {
            if (i + 1 >= argc) {
                throw std::runtime_error("error");
            }
            width = std::atoi(argv[i + 1]);
        }
    }
    switch (std::stoi(caseIdx)) {
        case 0:
            test_bitwise_bmt_gen_0(num, width);
            break;
        case 1:
            test_rand_ot_batch_for_bit_1();
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
        case 7:
            test_int_mux_7();
            break;
        case 9:
            test_ot_9();
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
        case 17:
            test_batch_less_17();
            break;
        case 18:
            test_bits_ot_18();
            break;
        case 19:
            test_arith_to_bool_batch_19();
            break;
        case 20:
            test_arith_to_bool_batch_conversion_20();
            break;
        case 21:
            test_arith_less_batch_21();
            break;
        case 22:
            test_arith_multiply_batch_22();
            break;
        case 23:
            test_arith_mutex_batch_23();
            break;
        case 24:
            test_bool_equal_batch_24();
            break;
        case 25:
            test_arith_equal_batch_25();
            break;
        case 26:
            test_equal_operators_comparison_26();
            break;
    }

    System::finalize();
    return 0;
}
