//
// Created by 杜建璋 on 2024/8/29.
//

#ifndef DEMO_TEST_CASES_H
#define DEMO_TEST_CASES_H

#include "../include/utils/Log.h"
#include "../include/comm/Comm.h"
#include "../include/utils/Math.h"
#include "../include/compute/single/arith/ArithAddOperator.h"
#include "../include/compute/single/arith/ArithMultiplyOperator.h"
#include "../include/intermediate/BmtGenerator.h"
#include "../include/intermediate/IntermediateDataSupport.h"
#include "../include/ot/BaseOtOperator.h"
#include "../include/ot/RandOtOperator.h"
#include "../include/ot/RandOtBatchOperator.h"
#include "../include/compute/single/arith/ArithLessOperator.h"
#include "../include/compute/single/bool/BoolAndOperator.h"
#include "../include/compute/single/bool/BoolToArithOperator.h"
#include "../include/compute/single/arith/ArithToBoolOperator.h"
#include "../include/secret/item/BoolSecret.h"
#include "../include/secret/item/ArithSecret.h"
#include "../include/compute/single/arith/ArithMutexOperator.h"
#include "../include/compute/single/bool/BoolLessOperator.h"
#include "../include/compute/single/bool/BoolMutexOperator.h"
#include "../include/secret/Secrets.h"
#include "../include/intermediate/BitwiseBmtGenerator.h"
#include "../include/parallel/ThreadPoolSupport.h"
#include "../include/compute/batch/bool/BoolAndBatchOperator.h"
#include "../include/compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"

inline void test_bitwise_bmt_gen_0(int num, int width) {
    if (Comm::isServer()) {
        auto start = System::currentTimeMillis();
        auto t = System::nextTask();

        auto b = BitwiseBmtBatchGenerator(num, width, t, 0).execute()->_bmts;
        auto end = System::currentTimeMillis();
        Log::i("total time: {}ms", end - start);
    }
}

inline void test_rand_ot_batch_for_bit_1() {
    if (Comm::isClient()) {
        return;
    }

    auto t = System::nextTask();

    std::vector<int64_t> ms0 = {0b10100011};
    std::vector<int64_t> ms1 = {0b01001010};
    std::vector<int64_t> choices = {0b10011110};

    auto e = RandOtBatchOperator(0, &ms0, &ms1, &choices, t, 0);
    e.execute();

    if (Comm::rank() == 1) {
        int64_t totalWrong = 0;
        for (int i = 0; i < e._results.size(); i++) {
            for (int j = 0; j < 64; j++) {
                // choose 1
                if (Math::getBit(choices[i], j)) {
                    if (Math::getBit(e._results[i], j) != Math::getBit(ms1[i], j)) {
                        totalWrong++;
                    }
                } else {
                    if (Math::getBit(e._results[i], j) != Math::getBit(ms0[i], j)) {
                        totalWrong++;
                    }
                }
            }
        }
        Log::i("total wrong bits: {}", totalWrong);
    }
}

inline void test_bmt_generation_2() {
    if (Comm::isServer()) {
        int i = 0;
        while (i++ < 20) {
            auto b = BmtGenerator(8, 10, 0).execute()->_bmt;
            Log::i("ai: " + std::to_string(static_cast<int8_t>(b._a)) + " bi: " + std::to_string(
                       static_cast<int8_t>(b._b)) + " ci: " + std::to_string(static_cast<int8_t>(b._c)));
        }
    }
}


inline void test_bitwise_bool_and_3() {
    int i = 0;
    int num = 1;
    auto t = System::nextTask();
    std::vector<std::future<void> > futures;
    futures.reserve(num);
    while (i++ < num) {
        futures.push_back(ThreadPoolSupport::submit([t, i] {
            int64_t x, y;
            if (Comm::isClient()) {
                x = Math::randInt();
                y = Math::randInt();
            }
            BoolAndOperator e(x, y, 64, t + i, 0, 2);
            e.execute()->reconstruct(2);
            if (Comm::isClient()) {
                if (e._result != (x & y)) {
                    Log::e("Wrong result. {}", e._result);
                }
            }
        }));
    }
    for (auto &f: futures) {
        f.wait();
    }
}


inline void test_arith_less_4() {
    auto t = System::nextTask();
    for (int i = 0; i < 100; i++) {
        int64_t x, y;
        if (Comm::isClient()) {
            x = Math::randInt(-1000, 1000);
            y = Math::randInt(-1000, 1000);
        }
        ArithLessOperator e(x, y, 32, t + i, 0, 2);
        e./*setBmts(&bmts)->*/execute()->reconstruct(2);

        if (Comm::isClient()) {
            bool r = e._result;
            if (r != (x < y)) {
                Log::i("Wrong idx: {}", i);
            }
        }
    }
}

inline void test_convertion_5() {
    int i = 0;
    auto t = System::nextTask();
    int num = 1;
    while (i++ < num) {
        int64_t x;
        if (Comm::isClient()) {
            x = Math::randInt();
        }
        auto bx = ArithToBoolOperator(x, 64, t + i, 0, 2)./*setBmts(&bc)->*/execute()->_zi;
        Log::i("bx: {}", bx);
        auto ret = BoolToArithOperator(bx, 64, t + i, 0, -1).execute()->reconstruct(2)->_result;
        if (Comm::isClient()) {
            if (ret != x) {
                Log::i("Wrong, x: {}, ret: {}", x, ret);
            }
        }
    }
}

inline void test_int_mux_7() {
    std::vector<std::future<void> > futures;
    auto t = System::nextTask();
    futures.reserve(100);
    for (int i = 0; i < 50; i++) {
        futures.push_back(ThreadPoolSupport::submit([t, i] {
            int64_t x, y;
            bool c;
            if (Comm::isClient()) {
                x = Math::randInt();
                y = Math::randInt();
                c = Math::randInt(0, 1);
            }

            ArithMutexOperator e1(x, y, c, 64, t + i, 0, 2);
            e1.execute()->reconstruct(2);
            auto r = e1._result;
            if (Comm::isClient()) {
                if (r != (c ? x : y)) {
                    Log::i("Wrong, x: {}, y:{}, c:{}, r:{}", x, y, c, std::to_string(r));
                }
            }
        }));
    }
    for (auto &f: futures) {
        f.wait();
    }
}

inline void test_ot_9() {
    int i = 0;
    std::vector<std::future<void> > futures;
    while (i++ < 1) {
        auto t = System::nextTask();
        if (Comm::isServer()) {
            std::vector<int64_t> m0;
            std::vector<int64_t> m1;
            std::vector<int> c;
            m0 = {20};
            m1 = {40};
            c = {1};
            RandOtBatchOperator e(0, &m0, &m1, &c, 32, t, 0);
            e.execute();
            RandOtOperator e1(0, 20, 40, 1, 32, t + 1, 0);
            e1.execute();
            if (Comm::rank() == 1) {
                // if (e._result != 40) {
                //     Log::e("Wrong: " + to_string(e._result));
                // }
                if (e1._result != 40) {
                    Log::i("Wrong: {}", e1._result);
                }
                if (e._results[0] != 40) {
                    Log::e("Wrong batch: " + std::to_string(e._results[0]));
                }
            }
        }
    }
    if (Comm::isServer()) {
        for (auto &f: futures) {
            f.wait();
        }
    }
}

inline void test_bool_comp_11() {
    for (int i = 0; i < 100; i++) {
        int x, y;
        int len = 64;
        if (Comm::isClient()) {
            x = Math::ring(Math::randInt(0, 100), len);
            y = Math::ring(Math::randInt(0, 100), len);
        }
        BoolLessOperator e(x, y, len, System::nextTask(), 0, 2);
        e.execute()->reconstruct(2);
        if (Comm::isClient()) {
            if (static_cast<uint64_t>(x) < static_cast<uint64_t>(y) != e._result) {
                Log::i("Wrong result: {}", e._result);
            } else {
                Log::i("Correct: {}, a: {}, b, {}", e._result, x, y);
            }
        }
    }
}

inline void test_bool_mux_12() {
    std::vector<std::future<void> > futures;
    auto t = System::nextTask();
    futures.reserve(100);
    for (int i = 0; i < 100; i++) {
        int64_t x, y;
        bool c;
        if (Comm::isClient()) {
            x = Math::randInt();
            y = Math::randInt();
            c = Math::randInt(0, 1);
        }

        BoolMutexOperator e1(x, y, c, 64, t + i, 0, 2);
        e1.execute()->reconstruct(2);
        auto r = e1._result;
        if (Comm::isClient()) {
            if (r != (c ? x : y)) {
                Log::i("Wrong, x: {}, y:{}, c:{}, r:{}", x, y, c, std::to_string(r));
            }
        }
    }
    for (auto &f: futures) {
        f.wait();
    }
}

inline void test_api_14() {
    int a, b;
    if (Comm::isClient()) {
        a = 40;
        b = 20;
    }
    auto t = System::nextTask();
    BitSecret res = ArithSecret(a, 32, t).share(2).lessThan(ArithSecret(b, 32, t).share(2)).reconstruct(2);
    if (Comm::isClient()) {
        Log::i("arith res: {}", res._data);
    }

    res = BoolSecret(a, 32, t, 0).share(2).lessThan(BoolSecret(b, 32, t, 0).share(2)).reconstruct(2);
    if (Comm::isClient()) {
        Log::i("bool res: {}", res._data);
    }
}

inline void test_batch_and_15() {
    std::vector<int64_t> a, b;
    if (Comm::isClient()) {
        for (int i = 0; i < 100; i++) {
            a.push_back(Math::randInt());
            b.push_back(Math::randInt());
        }
    }
    auto t = System::nextTask();
    auto r = BoolAndBatchOperator(&a, &b, 32, t, 0, 2).execute()->reconstruct(2)->_results;
    if (Comm::isClient()) {
        for (int i = 0; i < r.size(); i++) {
            if (Math::ring(a[i] & b[i], 32) != r[i]) {
                Log::i("Wrong: {}", r[i]);
            } else {
                Log::i("Correct: {}", r[i]);
            }
        }
    }
}

inline void test_batch_bool_mux_16() {
    std::vector<int64_t> a, b, c;
    int width = 32;
    if (Comm::isClient()) {
        for (int i = 0; i < 100; i++) {
            a.push_back(Math::randInt());
            b.push_back(Math::randInt());
            c.push_back(Math::randInt(0, 1));
        }
    }
    auto t = System::nextTask();
    auto r = BoolMutexBatchOperator(&a, &b, &c, width, t, 0, 2).execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        for (int i = 0; i < r.size(); i++) {
            if (Math::ring(c[i] ? a[i] : b[i], width) != r[i]) {
                Log::i("Wrong: {}", Math::toBinString<64>(r[i]));
            } else {
                Log::i("Correct: {}", Math::toBinString<64>(r[i]));
            }
        }
    }
}

inline void test_batch_less_17() {
    std::vector<int64_t> a, b;
    if (Comm::isClient()) {
        for (int i = 0; i < 100; i++) {
            a.push_back(Math::randInt());
            b.push_back(Math::randInt());
        }
    }
    auto t = System::nextTask();
    auto r = BoolLessBatchOperator(&a, &b, 64, t, 0, 2).execute()->reconstruct(2)->_results;
    auto r1 = BoolLessOperator(2, 5, 64, t, 0, 2).execute()->reconstruct(2)->_result;

    if (Comm::isClient()) {
        for (int i = 0; i < r.size(); i++) {
            if ((static_cast<uint64_t>(a[i]) < static_cast<uint64_t>(b[i])) != r[i]) {
                Log::i("a:{}, b:{}, Wrong: {} r1: {}", a[i], b[i], r[i], r1);
            } else {
                Log::i("a:{}, b:{}, Correct: {} r1: {}", a[i], b[i], r[i], r1);
            }
        }
    }
}

inline void test_bits_ot_18() {
    auto t = System::nextTask();
    if (Comm::isServer()) {
        std::vector<int64_t> m0;
        std::vector<int64_t> m1;
        std::vector<int64_t> c;
        m0 = {-1};
        m1 = {0b1001};
        c = {0b0011};
        RandOtBatchOperator e(0, &m0, &m1, &c, t, 0);
        e.execute();
        if (Comm::rank() == 1) {
            Log::i("result: {}", Math::toBinString<64>(e._results[0]));
            Log::i("result: {}", Math::toBinString<64>(e._results[1]));
        }
    }
}


#endif //DEMO_TEST_CASES_H
