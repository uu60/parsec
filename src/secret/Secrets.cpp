//
// Created by 杜建璋 on 2025/2/12.
//

#include "secret/Secrets.h"
#include "utils/System.h"
#include <cmath>

#include "accelerate/SimdSupport.h"
#include "compute/batch/bool/BoolLessBatchExecutor.h"
#include "compute/batch/bool/BoolMutexBatchExecutor.h"
#include "compute/single/bool/BoolLessExecutor.h"
#include "conf/Conf.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"


template<typename SecretT>
void compareAndSwap(std::vector<SecretT> &secrets, size_t i, size_t j, bool dir, int taskTag,
                    int msgTagOffset) {
    if (secrets[i]._padding && secrets[j]._padding) {
        return;
    }
    if ((secrets[i]._padding && dir) || (secrets[j]._padding && !dir)) {
        std::swap(secrets[i], secrets[j]);
        if constexpr (Conf::SORT_IN_PARALLEL) {
            std::atomic_thread_fence(std::memory_order_release);
        }
        return;
    }
    if (secrets[i]._padding || secrets[j]._padding) {
        return;
    }
    auto swap = secrets[i].task(taskTag).msg(msgTagOffset).lessThan(secrets[j]).not_();
    if (!dir) {
        swap = swap.not_();
    }
    if constexpr (Conf::TASK_BATCHING) {
        std::vector xs = {secrets[j]._data, secrets[i]._data};
        std::vector ys = {secrets[i]._data, secrets[j]._data};
        std::vector<int64_t> conds = {swap._data, swap._data};
        auto zis = BoolMutexBatchExecutor(xs, ys, conds, secrets[i]._width, taskTag, msgTagOffset,
                                          AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zis;
        secrets[i]._data = zis[0];
        secrets[j]._data = zis[1];
    } else {
        auto tempI = secrets[j].mux(secrets[i], swap);
        auto tempJ = secrets[i].mux(secrets[j], swap);
        secrets[i] = tempI;
        secrets[j] = tempJ;
    }

    // keep memory synchronized
    if constexpr (Conf::SORT_IN_PARALLEL) {
        std::atomic_thread_fence(std::memory_order_release);
    }
}

template<typename SecretT>
void compareAndSwapBatch(std::vector<SecretT> &secrets, size_t low, size_t mid, bool dir, int taskTag,
                         int msgTagOffset) {
    std::vector<size_t> comparing;
    comparing.reserve(mid);
    for (size_t i = low; i < low + mid; i++) {
        auto j = i + mid;
        if (secrets[i]._padding && secrets[j]._padding) {
            continue;
        }
        if ((secrets[i]._padding && dir) || (secrets[j]._padding && !dir)) {
            std::swap(secrets[i], secrets[j]);
            if constexpr (Conf::SORT_IN_PARALLEL) {
                std::atomic_thread_fence(std::memory_order_release);
            }
            continue;
        }
        if (secrets[i]._padding || secrets[j]._padding) {
            continue;
        }
        comparing.push_back(i);
    }

    std::vector<int64_t> xs, ys;
    int cc = static_cast<int>(comparing.size());
    xs.reserve(cc);
    ys.reserve(cc);
    for (int i = 0; i < cc; i++) {
        xs.push_back(secrets[comparing[i]]._data);
        ys.push_back(secrets[comparing[i] + mid]._data);
    }

    BoolLessBatchExecutor blbe(xs, ys, secrets[0]._width, taskTag, msgTagOffset,
                                                   AbstractSecureExecutor::NO_CLIENT_COMPUTE);

    auto zs = blbe.execute()->_zis;
    xs = std::move(blbe._xis);
    ys = std::move(blbe._yis);

    if (!dir) {
        if constexpr (Conf::ENABLE_SIMD) {
            zs = SimdSupport::xorVC(zs, Comm::rank());
        } else {
            for (auto &z : zs) {
                z = z ^ Comm::rank();
            }
        }
    } // zs now represents if needs swap

    xs.insert(xs.end(), ys.begin(), ys.end());
    ys.resize(xs.size());
    for (int i = cc; i < xs.size(); i++) {
        ys[i] = xs[i - cc];
    }
    zs.insert(zs.end(), zs.begin(), zs.end());

    zs = BoolMutexBatchExecutor(ys, xs, zs, secrets[0]._width, taskTag, msgTagOffset, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zis;

    for (int i = 0; i < cc; i++) {
        secrets[comparing[i]]._data = zs[i];
        secrets[comparing[i] + mid]._data = zs[i + cc];
    }
}

template<typename SecretT>
void bitonicMerge(std::vector<SecretT> &secrets, size_t low, size_t length, bool dir, int taskTag,
                  int msgTagOffset) {
    glow = low;
    if (length > 1) {
        size_t mid = length / 2;
        // for (size_t i = low; i < low + mid; i++) {
        //     compareAndSwap<SecretT>(secrets, i, i + mid, dir, taskTag, msgTagOffset);
        // }
        compareAndSwapBatch<SecretT>(secrets, low, mid, dir, taskTag, msgTagOffset);
        bitonicMerge<SecretT>(secrets, low, mid, dir, taskTag, msgTagOffset);
        bitonicMerge<SecretT>(secrets, low + mid, mid, dir, taskTag, msgTagOffset);
    }
}

template<typename SecretT>
void bitonicSort(std::vector<SecretT> &secrets, size_t low, size_t length, bool dir, int taskTag,
                 int msgTagOffset, int level) {
    if (length > 1) {
        size_t mid = length / 2;
        // std::future<void> f;
        // bool parallel = Conf::SORT_IN_PARALLEL && level < static_cast<int>(std::log2(std::thread::hardware_concurrency() / 3));
        // if (parallel) {
        //     f = ThreadPoolSupport::submit([&] {
        //         // int msgCount = std::max(BoolMutexBatchExecutor::msgTagCount(2, secrets[0]._width),
        //         //                     BoolLessExecutor::msgTagCount(secrets[0]._width));
        //         int msgCount = 2;
        //         bitonicSort<SecretT>(secrets, low + mid, mid, true, taskTag, msgTagOffset + length / 4 * msgCount, level + 1);
        //     });
        // } else {
        bitonicSort<SecretT>(secrets, low + mid, mid, true, taskTag, msgTagOffset, level + 1);
        // }
        bitonicSort<SecretT>(secrets, low, mid, false, taskTag, msgTagOffset, level + 1);

        // if (parallel) {
        //     f.wait();
        // }
        bitonicMerge<SecretT>(secrets, low, length, dir, taskTag, msgTagOffset);
    }
}

template<typename SecretT>
void compareAndSwapByBit(std::vector<SecretT> &secrets, size_t i, size_t j, int k, bool asc, int taskTag) {
    if (secrets[i]._padding && secrets[j]._padding) {
        return;
    }
    if (secrets[i]._padding && asc) {
        secrets[j]._padding = true;
        secrets[i]._data = secrets[j]._data;
        secrets[j]._padding = false;
        return;
    }
    if (secrets[j]._padding && !asc) {
    }
    if (secrets[i]._padding || secrets[j]._padding) {
        return;
    }

    auto i_bit_k = secrets[i].getBit(k);
    auto j_bit_k = secrets[j].getBit(k);

    auto swap_cond = i_bit_k.lessThan(j_bit_k).not_(); // i_bit_k > j_bit_k

    if (!asc) {
        swap_cond = swap_cond.not_();
    }

    std::vector xs = {secrets[j]._data, secrets[i]._data};
    std::vector ys = {secrets[i]._data, secrets[j]._data};
    std::vector<int64_t> conds = {swap_cond._data, swap_cond._data};
    auto zis = BoolMutexBatchExecutor(xs, ys, conds, secrets[i]._width, taskTag, 0,
                                      AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zis;
    secrets[i]._data = zis[0];
    secrets[j]._data = zis[1];
}

template<typename SecretT>
void insertionSortByBit(std::vector<SecretT> &secrets, int k, bool asc, int taskTag) {
    for (size_t i = 1; i < secrets.size(); ++i) {
        for (size_t j = i; j > 0; --j) {
            compareAndSwapByBit<SecretT>(secrets, j - 1, j, k, asc, taskTag);
        }
    }
}

template<typename SecretT>
void radixSort(std::vector<SecretT> &secrets, bool asc, int taskTag) {
    if (secrets.empty()) return;
    for (int k = 0; k < secrets[0]._width; ++k) {
        insertionSortByBit<SecretT>(secrets, k, asc, taskTag);
    }
}

template<typename SecretT>
void doSort(std::vector<SecretT> &secrets, bool asc, int taskTag) {
    if constexpr (Conf::SORT_METHOD == Consts::BITONIC) {
        size_t n = secrets.size();
        bool isPowerOf2 = (n > 0) && ((n & (n - 1)) == 0);
        size_t paddingCount = 0;
        if (!isPowerOf2) {
            SecretT p;
            p._padding = true;

            size_t nextPow2 = static_cast<size_t>(1) <<
                              static_cast<size_t>(std::ceil(std::log2(n)));
            secrets.resize(nextPow2, p);
            paddingCount = nextPow2 - n;
        }
        bitonicSort<SecretT>(secrets, 0, secrets.size(), asc, taskTag, 0, 0);
        if (paddingCount > 0) {
            secrets.resize(secrets.size() - paddingCount);
        }
    } else {
        radixSort<SecretT>(secrets, asc, taskTag);
    }
}

void Secrets::sort(std::vector<ArithSecret> &secrets, bool asc, int taskTag) {
    doSort<ArithSecret>(secrets, asc, taskTag);
}

void Secrets::sort(std::vector<BoolSecret> &secrets, bool asc, int taskTag) {
    doSort<BoolSecret>(secrets, asc, taskTag);
}
