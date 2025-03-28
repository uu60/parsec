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
#include "compute/single/bool/BoolMutexExecutor.h"
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
        if (Conf::SORT_IN_PARALLEL) {
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
    auto tempI = secrets[j].mux(secrets[i], swap);
    auto tempJ = secrets[i].mux(secrets[j], swap);
    secrets[i] = tempI;
    secrets[j] = tempJ;

    // keep memory synchronized
    if (Conf::SORT_IN_PARALLEL) {
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
            continue;
        }
        if (secrets[i]._padding || secrets[j]._padding) {
            continue;
        }
        comparing.push_back(i);
    }

    if (comparing.empty()) {
        return;
    }

    int cc = static_cast<int>(comparing.size());
    std::vector<int64_t> xs(cc), ys(cc);
    for (int i = 0; i < cc; i++) {
        xs[i] = secrets[comparing[i]]._data;
        ys[i] = secrets[comparing[i] + mid]._data;
    }

    auto zs = BoolLessBatchExecutor(&xs, &ys, secrets[0]._width, taskTag, msgTagOffset,
                                    AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zis;

    if (!dir) {
        for (auto &z: zs) {
            z = z ^ Comm::rank();
        }
    } // zs now represents if needs swap

    auto r0 = BoolMutexBatchExecutor(&xs, &ys, &zs, secrets[0]._width, taskTag, msgTagOffset).execute()->_zis;

    for (int i = 0; i < cc; i++) {
        secrets[comparing[i]]._data = r0[i];
        secrets[comparing[i] + mid]._data = r0[i + cc];
    }

    if (Conf::SORT_IN_PARALLEL) {
        std::atomic_thread_fence(std::memory_order_release);
    }
}

template<typename SecretT>
void bitonicMerge(std::vector<SecretT> &secrets, size_t low, size_t length, bool dir, int taskTag,
                  int msgTagOffset) {
    if (length > 1) {
        size_t mid = length / 2;
        if (Conf::ENABLE_TASK_BATCHING) {
            compareAndSwapBatch<SecretT>(secrets, low, mid, dir, taskTag, msgTagOffset);
        } else {
            for (size_t i = low; i < low + mid; i++) {
                compareAndSwap<SecretT>(secrets, i, i + mid, dir, taskTag, msgTagOffset);
            }
        }
        bitonicMerge<SecretT>(secrets, low, mid, dir, taskTag, msgTagOffset);
        bitonicMerge<SecretT>(secrets, low + mid, mid, dir, taskTag, msgTagOffset);
    }
}

template<typename SecretT>
void bitonicSort(std::vector<SecretT> &secrets, size_t low, size_t length, bool dir, int taskTag,
                 int msgTagOffset, int level) {
    // size_t n = secrets.size();
    // for (size_t size = 2; size <= n; size *= 2) {
    //     for (size_t subSize = size; subSize > 1; subSize /= 2) {
    //         size_t mid = subSize / 2;
    //         for (size_t low = 0; low < n; low += size) {
    //             bool block_dir = (((low / size) & 1) == 0) ? dir : !dir;
    //             for (size_t subLow = low; subLow < low + size; subLow += subSize) {
    //                 if (Conf::ENABLE_TASK_BATCHING) {
    //                     compareAndSwapBatch<SecretT>(secrets, subLow, mid, block_dir, taskTag, msgTagOffset);
    //                 } else {
    //                     for (size_t i = subLow; i < subLow + mid; i++) {
    //                         compareAndSwap<SecretT>(secrets, i, i + mid, block_dir, taskTag, msgTagOffset);
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }
    int n = secrets.size();
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xs, ys;
            std::vector<int64_t> xIdx, yIdx;
            std::vector<bool> ascs;
            int halfN = n / 2;
            xs.reserve(halfN);
            ys.reserve(halfN);
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            ascs.reserve(halfN);

            for (int i = 0; i < n; i++) {
                int l = i ^ j;
                if (l > i) {
                    bool asc = (i & k) == 0;
                    xs.push_back(secrets[i]._data);
                    xIdx.push_back(i);
                    ys.push_back(secrets[l]._data);
                    yIdx.push_back(l);
                    ascs.push_back(asc);
                }
            }

            auto zs = BoolLessBatchExecutor(&xs, &ys, secrets[0]._width, taskTag, msgTagOffset, AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zis;

            for (int i = 0; i < halfN; i++) {
                if (!ascs[i]) {
                    zs[i] = zs[i] ^ Comm::rank();
                }
            }

            zs = BoolMutexBatchExecutor(&xs, &ys, &zs, secrets[0]._width, taskTag, msgTagOffset).execute()->_zis;

            for (int i = 0; i < halfN; i++) {
                secrets[xIdx[i]]._data = zs[i];
                secrets[yIdx[i]]._data = zs[i + halfN];
            }
        }
    }
}

template<typename SecretT>
void doSort(std::vector<SecretT> &secrets, bool asc, int taskTag) {
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
}

void Secrets::sort(std::vector<ArithSecret> &secrets, bool asc, int taskTag) {
    doSort<ArithSecret>(secrets, asc, taskTag);
}

void Secrets::sort(std::vector<BoolSecret> &secrets, bool asc, int taskTag) {
    doSort<BoolSecret>(secrets, asc, taskTag);
}
