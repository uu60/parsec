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
    int num = 20;
    std::vector<std::future<void> > futures;
    futures.reserve(num);

    vector<Bmt> bmts;
    int l = 64;
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
        IntermediateDataSupport::prepareRot();
        int i = 0;
        while (i++ < 20) {
            auto b = BmtGenerator(8, 0, 0).execute()->_bmt;
            Log::i("ai: " + std::to_string(static_cast<int8_t>(b._a)) + " bi: " + std::to_string(
                       static_cast<int8_t>(b._b)) + " ci: " + std::to_string(static_cast<int8_t>(b._c)));
        }
    }
}


inline void test_bitwise_bool_and_3() {
    IntermediateDataSupport::prepareRot();
    IntermediateDataSupport::startGenerateBmtsAsync();
    int i = 0;
    int num = 1;
    auto t = System::nextTask();
    std::vector<std::future<void> > futures;
    futures.reserve(num);
    while (i++ < num) {
        futures.push_back(System::_threadPool.push([t, i](int _) {
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
    IntermediateDataSupport::prepareRot();
    IntermediateDataSupport::startGenerateBmtsAsync();

    std::vector<std::future<void> > futures;
    auto t = System::nextTask();
    futures.reserve(100);
    for (int i = 0; i < 50; i++) {
        futures.push_back(System::_threadPool.push([t, i](int _) {
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

//================== 测试函数：递归版 Bitonic Sort ==================//

inline void test_Sort_10() {
    // 1. 预备工作
    // IntermediateDataSupport::prepareRot();
    // IntermediateDataSupport::startGenerateBmtsAsync();
    std::vector<BoolSecret> arr;
    int num = 1000;

    // 2. 构造测试数据
    auto t = System::nextTask();
    for (int i = 0; i < num; i++) {
        // 这里简单构造 16,15,14,13,... 的序列
        arr.push_back(BoolSecret(num - i, 32, t).share(2));
    }

    // 测试计数用
    atomic_int a;

    // 3. 服务端执行「递归版 Bitonic Sort」
    if (Comm::isServer()) {
        auto start = System::currentTimeMillis();

        // 直接调用递归版本
        // dir = true 表示最后整体是升序
        Secrets::sort(arr, true);

        Log::i("total time: {}ms", System::currentTimeMillis() - start);
        Log::i("less than: {}ms", BoolLessExecutor::_totalTime);
        Log::i("comm: {}ms", Comm::_totalTime);
        Log::i("mux time: {}ms", BoolMutexExecutor::_totalTime);
        Log::i("bmt gen: {}ms", BitwiseBmtGenerator::_totalTime);
        Log::i("ot: {}ms", RandOtBatchExecutor::_totalTime);
        Log::i("bool and: {}ms", BoolAndExecutor::_totalTime);
    }

    // 4. 进行重构 (arithReconstruct) 取结果
    std::vector<BoolSecret> res;
    for (int i = 0; i < num; i++) {
        res.push_back(arr[i].task(3).reconstruct(2));
    }

    // 5. 客户端输出最终结果
    if (Comm::isClient()) {
        int last = INT_MIN;
        for (auto s: res) {
            if (s.get() < last) {
                Log::i("Wrong: {}", s.get());
            }
            last = s.get();
            // Log::i("{}", s.get());
        }
    }
}

inline void test_bool_comp_11() {
    IntermediateDataSupport::prepareRot();
    IntermediateDataSupport::startGenerateBmtsAsync();
    for (int i = 0; i < 100; i++) {
        int x, y;
        int len = 64;
        if (Comm::isClient()) {
            x = Math::ring(Math::randInt(), len);
            y = Math::ring(Math::randInt(), len);
        }
        BoolLessExecutor e(x, y, len, System::nextTask(), 0, 2);
        e.execute()->reconstruct(2);
        if (Comm::isClient()) {
            if (static_cast<uint64_t>(x) < static_cast<uint64_t>(y) != e._result) {
                Log::i("Wrong result: {}", e._result);
            }
        }
    }
}

inline void test_bool_mux_12() {
    IntermediateDataSupport::prepareRot();
    IntermediateDataSupport::startGenerateBmtsAsync();

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
    IntermediateDataSupport::prepareRot();
    IntermediateDataSupport::startGenerateBmtsAsync();

    int a, b;
    if (Comm::isClient()) {
        a = 40;
        b = 20;
    }
    auto t = System::nextTask();
    BitSecret res = ArithSecret(a, 32, t).share(2).lessThan(ArithSecret(b, 32, t).share(2)).reconstruct(2);
    if (Comm::isClient()) {
        Log::i("arith res: {}", res.get());
    }

    res = BoolSecret(a, 32, t).share(2).lessThan(BoolSecret(b, 32, t).share(2)).reconstruct(2);
    if (Comm::isClient()) {
        Log::i("bool res: {}", res.get());
    }
}


#endif //DEMO_TEST_CASES_H
