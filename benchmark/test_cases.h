//
// Created by 杜建璋 on 2024/8/29.
//

#ifndef DEMO_TEST_CASES_H
#define DEMO_TEST_CASES_H

#include "../include/utils/Log.h"
#include "../include/comm/Comm.h"
#include "../include/utils/Math.h"
#include "../include/compute/single/arith/ArithAddExecutor.h"
#include "../include/compute/single/arith/ArithMultiplyExecutor.h"
#include "../include/intermediate/BmtGenerator.h"
#include "../include/intermediate/IntermediateDataSupport.h"
#include "../include/ot/BaseOtExecutor.h"
#include "../include/ot/RandOtExecutor.h"
#include "../include/ot/RandOtBatchExecutor.h"
#include "../include/compute/single/arith/ArithLessExecutor.h"
#include "../include/compute/single/bool/BoolAndExecutor.h"
#include "../include/compute/single/bool/BoolToArithExecutor.h"
#include "../include/compute/single/arith/ArithToBoolExecutor.h"
#include "../include/secret/item/BoolSecret.h"
#include "../include/secret/item/ArithSecret.h"
#include "../include/compute/single/arith/ArithMutexExecutor.h"
#include "../include/compute/single/bool/BoolLessExecutor.h"
#include "../include/compute/single/bool/BoolMutexExecutor.h"
#include "../include/secret/Secrets.h"
#include "../include/intermediate/BitwiseBmtGenerator.h"
#include "../include/parallel/ThreadPoolSupport.h"
#include "../include/compute/batch/bool/BoolAndBatchExecutor.h"
#include "../include/compute/batch/bool/BoolMutexBatchExecutor.h"
#include "compute/batch/bool/BoolLessBatchExecutor.h"


using namespace std;

inline void test_arith_add_0() {
    int x, y;
    if (Comm::isClient()) {
        x = Math::randInt(-100, 100);
        y = Math::randInt(-100, 100);
        Log::i("Addend: " + std::to_string(x) + " and " + std::to_string(y));
    }
    ArithAddExecutor e(x, y, 32, System::nextTask(), 0, 2);
    e.execute()->reconstruct(2);
    if (!Comm::isServer()) {
        Log::i(std::to_string(static_cast<int>(e._result)));
    }
}

inline void test_arith_mul_parallel_1() {
    int num = 3;
    std::vector<std::future<void> > futures;
    futures.reserve(num);

    vector<Bmt> bmts;
    int l = 8;
    auto start = System::currentTimeMillis();
    int i = 0;
    while (i++ < num) {
        int64_t x, y;
        if (Comm::isClient()) {
            x = Math::randInt(0, 100);
            y = Math::randInt(0, 100);
        }
        ArithMultiplyExecutor e(x, y, l, 2 + i, 0, 2);
        // if (Comm::isServer()) {
        //     e.setBmt(&b);
        // }
        e.execute()->reconstruct(2);
        if (Comm::isClient()) {
            if (e._result != Math::ring(x * y, l)) {
                Log::e("Wrong answer: {} (should be {} * {} = {}), index: {}", e._result, x, y,
                       Math::ring(x * y, l), i);
            }
        }
    }
    for (auto &f: futures) {
        f.wait();
    }
    auto end = System::currentTimeMillis();
    Log::i("time: {}", end - start);
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
            BoolAndExecutor e(x, y, 64, t + i, 0, 2);
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
    // IntermediateDataSupport::prepareRot();
    // IntermediateDataSupport::startGenerateBmtsAsync();

    // std::vector<std::future<void> > futures;
    auto t = System::nextTask();
    // futures.reserve(100);
    for (int i = 0; i < 100; i++) {
        // auto bmts = Comm::isClient()
        //                 ? std::vector<Bmt>()
        //                 : IntermediateDataSupport::pollBmts(ArithLessExecutor::needBmtsWithBits(32).first, 32);
        // futures.push_back(System::_threadPool.push([i, t/*, &bmts*/](int _) {
        int64_t x, y;
        if (Comm::isClient()) {
            x = Math::randInt(-1000, 1000);
            y = Math::randInt(-1000, 1000);
        }
        ArithLessExecutor e(x, y, 32, t + i, 0, 2);
        e./*setBmts(&bmts)->*/execute()->reconstruct(2);

        if (Comm::isClient()) {
            bool r = e._result;
            if (r != (x < y)) {
                Log::i("Wrong idx: {}", i);
            }
        }
        // }));
    }
    // for (auto &f: futures) {
    //     f.wait();
    // }
}

inline void test_convertion_5() {
    // IntermediateDataSupport::prepareRot();
    // IntermediateDataSupport::startGenerateBmtsAsync();
    // IntermediateDataSupport::startGenerateABPairsAsyc();
    // std::vector<std::future<void> > futures;
    int i = 0;
    auto t = System::nextTask();
    int num = 1;
    while (i++ < num) {
        // auto bmts = Comm::isClient()
        //                 ? std::vector<Bmt>()
        //                 : IntermediateDataSupport::pollBmts(ArithToBoolExecutor::needBmtsWithBits(32).first, 32);
        // futures.push_back(System::_threadPool.push([i, t/*, bmts*/](int _) {
        // auto bc = bmts;
        int64_t x;
        if (Comm::isClient()) {
            x = Math::randInt();
        }
        auto bx = ArithToBoolExecutor(x, 64, t + i, 0, 2)./*setBmts(&bc)->*/execute()->_zi;
        Log::i("bx: {}", bx);
        auto ret = BoolToArithExecutor(bx, 64, t + i, 0, -1).execute()->reconstruct(2)->_result;
        if (Comm::isClient()) {
            if (ret != x) {
                Log::i("Wrong, x: {}, ret: {}", x, ret);
            }
        }
        // }));
    }
    // for (auto &f: futures) {
    //     f.wait();
    // }
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

            ArithMutexExecutor e1(x, y, c, 64, t + i, 0, 2);
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
    // IntermediateDataSupport::prepareRot();
    std::vector<std::future<void> > futures;
    while (i++ < 1) {
        auto t = System::nextTask();
        // futures.push_back(System::_threadPool.push([t, i](int _) {
        //     if (Comm::rank() <= 1) {
        //         int64_t m0;
        //         int64_t m1;
        //         if (Comm::rank() == 0) {
        //             m0 = 20;
        //             m1 = 40;
        //         }
        //         BaseOtExecutor e(0, m0, m1, 1, 32, t, 0);
        //         e.execute();
        //         if (Comm::rank() == 1) {
        //             if (e._result != 40) {
        //                 Log::e("Wrong: " + to_string(e._result));
        //             }
        //         }
        //     }
        // }));
        if (Comm::isServer()) {
            std::vector<int64_t> m0;
            std::vector<int64_t> m1;
            std::vector<int> c;
            m0 = {20};
            m1 = {40};
            c = {1};
            RandOtBatchExecutor e(0, &m0, &m1, &c, 32, t, 0);
            e.execute();
            RandOtExecutor e1(0, 20, 40, 1, 32, t + 1, 0);
            e1.execute();
            if (Comm::rank() == 1) {
                // if (e._result != 40) {
                //     Log::e("Wrong: " + to_string(e._result));
                // }
                if (e1._result != 40) {
                    Log::i("Wrong: {}", e1._result);
                }
                if (e._results[0] != 40) {
                    Log::e("Wrong batch: " + to_string(e._results[0]));
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


inline void test_Sort_10() {
    std::vector<BoolSecret> arr;
    int num = 1000000;

    auto t = System::nextTask();
    for (int i = 0; i < num; i++) {
        arr.push_back(BoolSecret(num - i, 64, t, 0).share(2));
    }

    if (Comm::isServer()) {
        auto start = System::currentTimeMillis();

        Secrets::sort(arr, true, t);

        Log::i("SIMD: {}", Conf::ENABLE_SIMD);
        Log::i("total time: {}ms", System::currentTimeMillis() - start);
        Log::i("less than: {}ms", BoolLessExecutor::_totalTime + BoolLessBatchExecutor::_totalTime);
        Log::i("bool and: {}ms", BoolAndExecutor::_totalTime + BoolAndBatchExecutor::_totalTime);
        Log::i("comm: {}ms", Comm::_totalTime);
        Log::i("mux time: {}ms", BoolMutexExecutor::_totalTime + BoolMutexBatchExecutor::_totalTime);
        Log::i("bmt gen: {}ms", BitwiseBmtGenerator::_totalTime);
        Log::i("ot: {}ms", RandOtBatchExecutor::_totalTime);
    }

    std::vector<BoolSecret> res;
    for (int i = 0; i < num; i++) {
        res.push_back(arr[i].task(3).reconstruct(2));
    }

    if (Comm::isClient()) {
        int last = INT_MIN;
        for (auto s: res) {
            if (s._data <= last) {
                Log::i("Wrong: {}", s._data);
            }
            last = s._data;
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
        BoolLessExecutor e(x, y, len, System::nextTask(), 0, 2);
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

        BoolMutexExecutor e1(x, y, c, 64, t + i, 0, 2);
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

//================== 测试函数：递归版 Bitonic Sort ==================/

void test_api_14() {
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

void test_batch_and_15() {
    std::vector<int64_t> a, b;
    if (Comm::isClient()) {
        a = {0b1111, 0b0000, 0b0011, Math::randInt(), Math::randInt()};
        b = {0b1010, 0b1111, 0b0101, Math::randInt(), Math::randInt()};
    }
    auto t = System::nextTask();
    auto r = BoolAndBatchExecutor(&a, &b, 64, t, 0, 2).execute()->reconstruct(2)->_results;
    if (Comm::isClient()) {
        for (int i = 0; i < r.size(); i++) {
            if ((a[i] & b[i]) != r[i]) {
                Log::i("Wrong: {}", r[i]);
            } else {
                Log::i("Correct: {}", r[i]);
            }
        }
    }
}

void test_batch_bool_mux_16() {
    std::vector<int64_t> a, b, c;
    if (Comm::isClient()) {
        a = {0b1111, 0b0000, 0b0011, Math::randInt(), Math::randInt()};
        b = {0b1010, 0b1111, 0b0101, Math::randInt(), Math::randInt()};
        c = {0, 1, 0, Math::randInt(0, 1), Math::randInt(0, 1)};
    }
    auto t = System::nextTask();
    // BoolMutexBatchExecutor e(a, b, c, 64, t, 0, 2);
    // auto r = e.execute()->reconstruct(2);
    // Log::i("?? {}", r == &e);
    auto r = BoolMutexBatchExecutor(&a, &b, &c, 64, t, 0, 2).execute()->reconstruct(2)->_results;

    if (Comm::isClient()) {
        for (int i = 0; i < r.size(); i++) {
            if ((c[i] ? a[i] : b[i]) != r[i]) {
                Log::i("Wrong: {}", Math::toBinString<64>(r[i]));
            } else {
                Log::i("Correct: {}", Math::toBinString<64>(r[i]));
            }
        }
    }
}

void test_batch_less_17() {
    std::vector<int64_t> a, b;
    if (Comm::isClient()) {
        a = {2, 0, 10, Math::randInt(0, 100), Math::randInt(0, 100), Math::randInt(0, 100), Math::randInt(0, 100)};
        b = {5, 1, 10, Math::randInt(0, 100), Math::randInt(0, 100), Math::randInt(0, 100), Math::randInt(0, 100)};
    }
    auto t = System::nextTask();
    auto r = BoolLessBatchExecutor(&a, &b, 64, t, 0, 2).execute()->reconstruct(2)->_results;
    auto r1 = BoolLessExecutor(2, 5, 64, t, 0, 2).execute()->reconstruct(2)->_result;

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


#endif //DEMO_TEST_CASES_H
