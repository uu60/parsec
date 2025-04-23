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

using BatchOutput = std::tuple<
    std::vector<int>,
    std::vector<int>,
    std::vector<int64_t>
>;

template<typename SecretT>
void bitonicSort(std::vector<SecretT> &secrets, bool asc, int taskTag, int msgTagOffset) {
    if (Conf::BATCH_SIZE <= 0) {
        int n = secrets.size();
        for (int k = 2; k <= n; k *= 2) {
            for (int j = k / 2; j > 0; j /= 2) {
                std::vector<int64_t> xs, ys;
                std::vector<int64_t> xIdx, yIdx;
                std::vector<bool> ascs;
                int halfN = n / 2;
                xs.resize(halfN);
                ys.resize(halfN);
                xIdx.resize(halfN);
                yIdx.resize(halfN);
                ascs.resize(halfN);
                int idx = 0;

                for (int i = 0; i < n; i++) {
                    int l = i ^ j;
                    if (l <= i) {
                        continue;
                    }
                    bool dir = (i & k) == 0;
                    if (secrets[i]._padding && secrets[l]._padding) {
                        continue;
                    }
                    if ((secrets[i]._padding && dir) || (secrets[l]._padding && !dir)) {
                        std::swap(secrets[i], secrets[l]);
                        continue;
                    }
                    if (secrets[i]._padding || secrets[l]._padding) {
                        continue;
                    }
                    xs[idx] = secrets[i]._data;
                    xIdx[idx] = i;
                    ys[idx] = secrets[l]._data;
                    yIdx[idx] = l;
                    ascs[idx] = dir ^ !asc;
                    idx++;
                }

                auto zs = BoolLessBatchExecutor(&xs, &ys, secrets[0]._width, taskTag, msgTagOffset,
                                                AbstractSecureExecutor::NO_CLIENT_COMPUTE).execute()->_zis;

                int comparingCount = static_cast<int>(xs.size());
                for (int i = 0; i < comparingCount; i++) {
                    if (!ascs[i]) {
                        zs[i] = zs[i] ^ Comm::rank();
                    }
                }

                zs = BoolMutexBatchExecutor(&xs, &ys, &zs, secrets[0]._width, taskTag, msgTagOffset).execute()->_zis;

                for (int i = 0; i < comparingCount; i++) {
                    secrets[xIdx[i]]._data = zs[i];
                    secrets[yIdx[i]]._data = zs[i + comparingCount];
                }
            }
        }
    } else {
        int batchSize = Conf::BATCH_SIZE;
        int n = secrets.size();
        for (int k = 2; k <= n; k *= 2) {
            for (int j = k / 2; j > 0; j /= 2) {
                int comparingCount = 0;
                for (int i = 0; i < n; ++i) {
                    int l = i ^ j;
                    if (l <= i) continue;
                    bool dir = (i & k) == 0;
                    if (secrets[i]._padding && secrets[l]._padding) continue;
                    if ((secrets[i]._padding && dir) || (secrets[l]._padding && !dir)) {
                        std::swap(secrets[i], secrets[l]);
                        continue;
                    }
                    if (secrets[i]._padding || secrets[l]._padding) continue;
                    ++comparingCount;
                }

                int numBatches = (comparingCount + batchSize - 1) / batchSize;
                std::vector<std::vector<int64_t> > xsBatches(numBatches), ysBatches(numBatches);
                std::vector<std::vector<int> > xIdxBatches(numBatches), yIdxBatches(numBatches);
                std::vector<std::vector<bool> > ascsBatches(numBatches);

                for (int b = 0; b < numBatches; ++b) {
                    xsBatches[b].reserve(batchSize);
                    ysBatches[b].reserve(batchSize);
                    xIdxBatches[b].reserve(batchSize);
                    yIdxBatches[b].reserve(batchSize);
                    ascsBatches[b].reserve(batchSize);
                }

                int count = 0;
                for (int i = 0; i < n; ++i) {
                    int l = i ^ j;
                    if (l <= i) continue;
                    bool dir = (i & k) == 0;
                    if (secrets[i]._padding && secrets[l]._padding) continue;
                    if ((secrets[i]._padding && dir) || (secrets[l]._padding && !dir)) {
                        continue;
                    }
                    if (secrets[i]._padding || secrets[l]._padding) continue;

                    int b = count / batchSize;
                    xsBatches[b].push_back(secrets[i]._data);
                    ysBatches[b].push_back(secrets[l]._data);
                    xIdxBatches[b].push_back(i);
                    yIdxBatches[b].push_back(l);
                    ascsBatches[b].push_back(dir ^ !asc);
                    ++count;
                }

                std::vector<std::future<BatchOutput> > futures;
                futures.reserve(numBatches);

                for (int b = 0; b < numBatches; ++b) {
                    futures.emplace_back(
                        ThreadPoolSupport::submit([&, b]() -> BatchOutput {
                            auto &xsB = xsBatches[b];
                            auto &ysB = ysBatches[b];
                            auto &iB = xIdxBatches[b];
                            auto &jB = yIdxBatches[b];
                            auto &ascB = ascsBatches[b];
                            int sz = static_cast<int>(xsB.size());

                            int offset = std::max(BoolLessBatchExecutor::msgTagCount(), BoolMutexBatchExecutor::msgTagCount());
                            auto zs1 = BoolLessBatchExecutor(
                                &xsB, &ysB,
                                secrets[0]._width,
                                taskTag, msgTagOffset + offset * b,
                                AbstractSecureExecutor::NO_CLIENT_COMPUTE
                            ).execute()->_zis;

                            for (int t = 0; t < sz; ++t) {
                                if (!ascB[t]) {
                                    zs1[t] ^= Comm::rank();
                                }
                            }

                            auto zs2 = BoolMutexBatchExecutor(
                                &xsB, &ysB, &zs1,
                                secrets[0]._width,
                                taskTag, msgTagOffset + offset * b
                            ).execute()->_zis;

                            return std::make_tuple(iB, jB, zs2);
                        })
                    );
                }

                for (auto &fut: futures) {
                    auto [iB, jB, zs2] = fut.get();
                    int sz = static_cast<int>(iB.size());
                    for (int t = 0; t < sz; ++t) {
                        secrets[iB[t]]._data = zs2[t];
                        secrets[jB[t]]._data = zs2[t + sz];
                    }
                }
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
    bitonicSort<SecretT>(secrets, asc, taskTag, 0);
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
