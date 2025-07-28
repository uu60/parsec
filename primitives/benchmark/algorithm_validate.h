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
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/arith/ArithToBoolBatchOperator.h"
#include "compute/batch/arith/ArithMultiplyBatchOperator.h"
#include "compute/batch/arith/ArithMutexBatchOperator.h"
#include "compute/batch/arith/ArithEqualBatchOperator.h"
#include "compute/batch/bool/BoolEqualBatchOperator.h"
#include "intermediate/BitwiseBmtBatchGenerator.h"
#include "utils/StringUtils.h"

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
    bool testBatch = true;
    if (Comm::isClient()) {
        for (int i = 0; i < 1000000; i++) {
            a.push_back(Math::randInt());
            b.push_back(Math::randInt());
        }
    }

    auto t = System::nextTask();
    auto as = Secrets::boolShare(a, 2, 64, t);
    auto bs = Secrets::boolShare(b, 2, 64, t);

    std::vector<int64_t> z(as.size());
    if (Comm::isServer()) {
        auto start = System::currentTimeMillis();

        std::vector<BitwiseBmt> bmts;
        if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
            bmts = IntermediateDataSupport::pollBitwiseBmts(BoolLessBatchOperator::bmtCount(as.size(), 64), 64);
        }

        if (testBatch) {
            int batch_size = Conf::BATCH_SIZE;
            int batch_num = (as.size() + batch_size - 1) / batch_size;
            std::vector<std::future<std::vector<int64_t> > > futures(batch_num);

            for (int i = 0; i < batch_num; i++) {
                futures[i] = ThreadPoolSupport::submit([&, i] {
                    std::vector<int64_t> batch_a, batch_b;
                    batch_a.reserve(batch_size);
                    batch_b.reserve(batch_size);

                    for (int ii = i * batch_size; ii < (i + 1) * batch_size; ii++) {
                        batch_a.push_back(as[ii]);
                        batch_b.push_back(bs[ii]);
                    }

                    std::vector<BitwiseBmt> batch_bmts;
                    batch_bmts.reserve(BoolLessBatchOperator::bmtCount(batch_size, 64));
                    if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                        for (int ii = i * BoolLessBatchOperator::bmtCount(batch_size, 64);
                             ii < (i + 1) * BoolLessBatchOperator::bmtCount(batch_size, 64); ii++) {
                            batch_bmts.push_back(bmts[ii]);
                            if (ii == bmts.size() - 1) {
                                break;
                            }
                        }
                    }
                    return BoolLessBatchOperator(&batch_a, &batch_b, 64, t, BoolLessBatchOperator::tagStride() * i, -1).
                            setBmts(Conf::BMT_METHOD == Conf::BMT_BACKGROUND ? &batch_bmts : nullptr)->execute()->_zis;
                });
            }

            int cur = 0;
            for (auto &f: futures) {
                auto batch_res = f.get();
                for (int i = 0; i < batch_res.size(); i++) {
                    z[cur++] = batch_res[i];
                }
            }
        } else {
            BoolLessBatchOperator op(&as, &bs, 64, 0, 0, -1);
            z = op.execute()->_zis;
        }
        Log::i("time: {}ms", System::currentTimeMillis() - start);
    }

    auto res = Secrets::boolReconstruct(z, 2, 64, t);
    if (Comm::isClient()) {
        for (int i = 0; i < res.size(); i++) {
            if ((static_cast<uint64_t>(a[i]) < static_cast<uint64_t>(b[i])) != res[i]) {
                Log::i("a:{}, b:{}, Wrong: {} r1: {}", a[i], b[i]);
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

inline void test_arith_to_bool_batch_19() {
    std::vector<int64_t> a;
    int width = 32;
    int testSize = 50;

    if (Comm::isClient()) {
        for (int i = 0; i < testSize; i++) {
            // Generate test values in a reasonable range for the given width
            a.push_back(Math::randInt(0, (1LL << (width - 1)) - 1));
        }
    }

    auto t = System::nextTask();

    // Test ArithToBoolBatchOperator
    auto batchResult = ArithToBoolBatchOperator(&a, width, t, 0, 2).execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        Log::i("Testing ArithToBoolBatchOperator with {} values, width {}", testSize, width);

        int correctCount = 0;
        int wrongCount = 0;

        for (int i = 0; i < testSize; i++) {
            // Test against single operator for verification
            auto singleResult = ArithToBoolOperator(a[i], width, t + i + 1000, 0, 2).execute()->reconstruct(2)->_result;

            if (batchResult[i] == singleResult) {
                correctCount++;
                Log::i("Test {}: CORRECT - input: {}, batch: {}, single: {}",
                       i, a[i], batchResult[i], singleResult);
            } else {
                wrongCount++;
                Log::e("Test {}: WRONG - input: {}, batch: {}, single: {}",
                       i, a[i], batchResult[i], singleResult);
            }
        }

        Log::i("ArithToBoolBatchOperator Test Summary: {} correct, {} wrong out of {} tests",
               correctCount, wrongCount, testSize);

        if (wrongCount == 0) {
            Log::i("✅ All ArithToBoolBatchOperator tests PASSED!");
        } else {
            Log::e("❌ ArithToBoolBatchOperator tests FAILED with {} errors", wrongCount);
        }
    }
}

inline void test_arith_to_bool_batch_conversion_20() {
    std::vector<int64_t> a;
    int width = 16; // Use smaller width for easier verification
    int testSize = 10;

    if (Comm::isClient()) {
        // Use specific test values for better verification
        a = {0, 1, 2, 3, 15, 16, 255, 256, 1000, 65535};
    }

    auto t = System::nextTask();

    ArithToBoolBatchOperator op(&a, width, t, 0, 2);
    // Convert arith to bool using batch operator
    auto boolResults = op.execute()->reconstruct(2)->_results;

    Log::i("boolResults: {}", StringUtils::vecString(boolResults));
    Log::i("zis: {}", StringUtils::vecString(op._zis));
}

inline void test_arith_less_batch_21() {
    std::vector<int64_t> a, b;
    int width = 16; // Use smaller width for easier verification
    int testSize = 10;

    if (Comm::isClient()) {
        // Use specific test values for better verification
        a = {0, 1, 2, 3, 15, 16, 255, 256, 1000, 65535};
        b = {0, -1, 3, -3, 16, -16, 256, -256, 1001, 65534};
    }

    auto t = System::nextTask();

    ArithLessBatchOperator op(&a, &b, width, t, 0, 2);
    // Convert arith to bool using batch operator
    auto boolResults = op.execute()->reconstruct(2)->_results;

    Log::i("boolResults: {}", StringUtils::vecString(boolResults));
    Log::i("zis: {}", StringUtils::vecString(op._zis));
}

inline void test_arith_multiply_batch_22() {
    std::vector<int64_t> a, b;
    int width = 32;
    int testSize = 20;

    if (Comm::isClient()) {
        Log::i("Testing ArithMultiplyBatchOperator with {} values, width {}", testSize, width);

        // Generate test values in a reasonable range for the given width
        for (int i = 0; i < testSize; i++) {
            a.push_back(Math::randInt(1, 1000)); // Use smaller values to avoid overflow
            b.push_back(Math::randInt(1, 1000));
        }

        Log::i("Input vectors a: {}", StringUtils::vecString(a));
        Log::i("Input vectors b: {}", StringUtils::vecString(b));
    }

    auto t = System::nextTask();

    // Test ArithMultiplyBatchOperator
    ArithMultiplyBatchOperator batchOp(&a, &b, width, t, 0, 2);
    auto batchResults = batchOp.execute()->reconstruct(2)->_results;
    if (Comm::isClient()) {
        Log::i("batchResults: {}", StringUtils::vecString(batchResults));
    }
}

inline void test_arith_mutex_batch_23() {
    std::vector<int64_t> a, b, c;
    int width = 32;
    int testSize = 20;

    if (Comm::isClient()) {
        Log::i("Testing ArithMutexBatchOperator with {} values, width {}", testSize, width);

        // Generate test values
        for (int i = 0; i < testSize; i++) {
            a.push_back(Math::randInt(1, 1000));
            b.push_back(Math::randInt(1, 1000));
            c.push_back(Math::randInt(0, 1)); // Boolean condition
        }

        Log::i("Input vectors a: {}", StringUtils::vecString(a));
        Log::i("Input vectors b: {}", StringUtils::vecString(b));
        Log::i("Input vectors c: {}", StringUtils::vecString(c));
    }

    auto t = System::nextTask();

    // Test ArithMutexBatchOperator
    ArithMutexBatchOperator batchOp(&a, &b, &c, width, t, 0, 2);
    auto batchResults = batchOp.execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        Log::i("batchResults: {}", StringUtils::vecString(batchResults));
    }
}

inline void test_bool_equal_batch_24() {
    std::vector<int64_t> a, b;
    int width = 32;
    int testSize = 20;

    if (Comm::isClient()) {
        Log::i("Testing BoolEqualBatchOperator with {} values, width {}", testSize, width);

        // Generate test values with some equal pairs for better testing
        for (int i = 0; i < testSize; i++) {
            if (i % 3 == 0) {
                // Make some pairs equal
                int64_t val = Math::randInt(0, 1000);
                a.push_back(val);
                b.push_back(val);
            } else {
                // Make some pairs different
                a.push_back(Math::randInt(0, 1000));
                b.push_back(Math::randInt(0, 1000));
            }
        }

        Log::i("Input vectors a: {}", StringUtils::vecString(a));
        Log::i("Input vectors b: {}", StringUtils::vecString(b));
    }

    auto t = System::nextTask();

    // Test BoolEqualBatchOperator
    BoolEqualBatchOperator batchOp(&a, &b, width, t, 0, 2);
    auto batchResults = batchOp.execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        Log::i("BoolEqualBatchOperator results: {}", StringUtils::vecString(batchResults));
        
        // Verify results
        int correctCount = 0;
        int wrongCount = 0;
        
        for (int i = 0; i < testSize; i++) {
            // Convert to unsigned for proper comparison
            uint64_t ua = static_cast<uint64_t>(Math::ring(a[i], width));
            uint64_t ub = static_cast<uint64_t>(Math::ring(b[i], width));
            bool expected = (ua == ub);
            bool actual = (batchResults[i] == 1);
            
            if (expected == actual) {
                correctCount++;
                Log::i("Test {}: CORRECT - a: {}, b: {}, expected: {}, actual: {}", 
                       i, ua, ub, expected, actual);
            } else {
                wrongCount++;
                Log::e("Test {}: WRONG - a: {}, b: {}, expected: {}, actual: {}", 
                       i, ua, ub, expected, actual);
            }
        }
        
        Log::i("BoolEqualBatchOperator Test Summary: {} correct, {} wrong out of {} tests",
               correctCount, wrongCount, testSize);
               
        if (wrongCount == 0) {
            Log::i("✅ All BoolEqualBatchOperator tests PASSED!");
        } else {
            Log::e("❌ BoolEqualBatchOperator tests FAILED with {} errors", wrongCount);
        }
    }
}

inline void test_arith_equal_batch_25() {
    std::vector<int64_t> a, b;
    int width = 32;
    int testSize = 20;

    if (Comm::isClient()) {
        Log::i("Testing ArithEqualBatchOperator with {} values, width {}", testSize, width);

        // Generate test values with some equal pairs for better testing
        for (int i = 0; i < testSize; i++) {
            if (i % 3 == 0) {
                // Make some pairs equal
                int64_t val = Math::randInt(-500, 500);
                a.push_back(val);
                b.push_back(val);
            } else {
                // Make some pairs different
                a.push_back(Math::randInt(-1000, 1000));
                b.push_back(Math::randInt(-1000, 1000));
            }
        }

        Log::i("Input vectors a: {}", StringUtils::vecString(a));
        Log::i("Input vectors b: {}", StringUtils::vecString(b));
    }

    auto t = System::nextTask();

    // Test ArithEqualBatchOperator
    ArithEqualBatchOperator batchOp(&a, &b, width, t, 0, 2);
    auto batchResults = batchOp.execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        Log::i("ArithEqualBatchOperator results: {}", StringUtils::vecString(batchResults));
        
        // Verify results
        int correctCount = 0;
        int wrongCount = 0;
        
        for (int i = 0; i < testSize; i++) {
            // For arithmetic comparison, use signed comparison
            int64_t sa = Math::ring(a[i], width);
            int64_t sb = Math::ring(b[i], width);
            bool expected = (sa == sb);
            bool actual = (batchResults[i] == 1);
            
            if (expected == actual) {
                correctCount++;
                Log::i("Test {}: CORRECT - a: {}, b: {}, expected: {}, actual: {}", 
                       i, sa, sb, expected, actual);
            } else {
                wrongCount++;
                Log::e("Test {}: WRONG - a: {}, b: {}, expected: {}, actual: {}", 
                       i, sa, sb, expected, actual);
            }
        }
        
        Log::i("ArithEqualBatchOperator Test Summary: {} correct, {} wrong out of {} tests",
               correctCount, wrongCount, testSize);
               
        if (wrongCount == 0) {
            Log::i("✅ All ArithEqualBatchOperator tests PASSED!");
        } else {
            Log::e("❌ ArithEqualBatchOperator tests FAILED with {} errors", wrongCount);
        }
    }
}

inline void test_equal_operators_comparison_26() {
    std::vector<int64_t> a, b;
    int width = 32;
    int testSize = 15;

    if (Comm::isClient()) {
        Log::i("Testing comparison between ArithEqualBatchOperator and BoolEqualBatchOperator with {} values, width {}", testSize, width);

        // Generate test values with some equal pairs for better testing
        for (int i = 0; i < testSize; i++) {
            if (i % 3 == 0) {
                // Make some pairs equal
                int64_t val = Math::randInt(0, 500);
                a.push_back(val);
                b.push_back(val);
            } else {
                // Make some pairs different
                a.push_back(Math::randInt(0, 1000));
                b.push_back(Math::randInt(0, 1000));
            }
        }

        Log::i("Input vectors a: {}", StringUtils::vecString(a));
        Log::i("Input vectors b: {}", StringUtils::vecString(b));
    }

    auto t = System::nextTask();

    // Test ArithEqualBatchOperator
    ArithEqualBatchOperator arithOp(&a, &b, width, t, 0, 2);
    auto arithResults = arithOp.execute()->reconstruct(2)->_results;

    // Test BoolEqualBatchOperator
    BoolEqualBatchOperator boolOp(&a, &b, width, t, 0, 2);
    auto boolResults = boolOp.execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        Log::i("ArithEqual results: {}", StringUtils::vecString(arithResults));
        Log::i("BoolEqual results:  {}", StringUtils::vecString(boolResults));
        
        // Compare results between the two operators
        int matchCount = 0;
        int mismatchCount = 0;
        
        for (int i = 0; i < testSize; i++) {
            int64_t sa = Math::ring(a[i], width);
            int64_t sb = Math::ring(b[i], width);
            bool expected = (sa == sb);
            bool arithActual = (arithResults[i] == 1);
            bool boolActual = (boolResults[i] == 1);
            
            if (arithActual == boolActual && arithActual == expected) {
                matchCount++;
                Log::i("Test {}: MATCH - a: {}, b: {}, expected: {}, arith: {}, bool: {}", 
                       i, sa, sb, expected, arithActual, boolActual);
            } else {
                mismatchCount++;
                Log::e("Test {}: MISMATCH - a: {}, b: {}, expected: {}, arith: {}, bool: {}", 
                       i, sa, sb, expected, arithActual, boolActual);
            }
        }
        
        Log::i("Equal Operators Comparison Summary: {} matches, {} mismatches out of {} tests",
               matchCount, mismatchCount, testSize);
               
        if (mismatchCount == 0) {
            Log::i("✅ All Equal Operators Comparison tests PASSED!");
        } else {
            Log::e("❌ Equal Operators Comparison tests FAILED with {} errors", mismatchCount);
        }
    }
}


#endif //DEMO_TEST_CASES_H
