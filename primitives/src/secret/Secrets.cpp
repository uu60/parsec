
#include "secret/Secrets.h"
#include <cmath>

#include "accelerate/SimdSupport.h"
#include "compute/batch/arith/ArithBatchOperator.h"
#include "compute/batch/arith/ArithLessBatchOperator.h"
#include "compute/batch/arith/ArithMutexBatchOperator.h"
#include "compute/batch/bool/BoolLessBatchOperator.h"
#include "compute/batch/bool/BoolMutexBatchOperator.h"
#include "compute/single/bool/BoolMutexOperator.h"
#include "conf/Conf.h"
#include "intermediate/IntermediateDataSupport.h"
#include "parallel/ThreadPoolSupport.h"
#include "utils/Log.h"

using BatchOutput = std::tuple<
    std::vector<int>,
    std::vector<int>,
    std::vector<int64_t>
>;

void bitonicSortBoolSingleBatch(std::vector<BoolSecret> &secrets, bool asc, int taskTag, int msgTagOffset) {
    size_t n = secrets.size();
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xs, ys;
            std::vector<int> xIdx, yIdx;
            std::vector<bool> ascs;
            size_t halfN = n / 2;
            xs.reserve(halfN);
            ys.reserve(halfN);
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            ascs.reserve(halfN);

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
                xs.push_back(secrets[i]._data);
                xIdx.push_back(i);
                ys.push_back(secrets[l]._data);
                yIdx.push_back(l);
                ascs.push_back(dir ^ !asc);
            }

            auto zs = BoolLessBatchOperator(&xs, &ys, secrets[0]._width, taskTag, msgTagOffset,
                                            SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            int comparingCount = static_cast<int>(xs.size());
            for (int i = 0; i < comparingCount; i++) {
                if (!ascs[i]) {
                    zs[i] = zs[i] ^ Comm::rank();
                }
            }

            zs = BoolMutexBatchOperator(&xs, &ys, &zs, secrets[0]._width, taskTag, msgTagOffset).execute()->_zis;

            for (int i = 0; i < comparingCount; i++) {
                secrets[xIdx[i]]._data = zs[i];
                secrets[yIdx[i]]._data = zs[i + comparingCount];
            }
        }
    }
}

void bitonicSortBoolSplittedBatches(std::vector<BoolSecret> &secrets, bool asc, int taskTag, int msgTagOffset) {
    int batchSize = Conf::BATCH_SIZE;
    size_t n = secrets.size();
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            int comparingCount = 0;
            for (int i = 0; i < n; ++i) {
                int l = i ^ j;
                if (l <= i) {
                    continue;
                }
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
                std::shared_ptr<std::vector<BitwiseBmt>> bmtLessB, bmtMuxB;

                if (Conf::BMT_METHOD == Conf::BMT_BACKGROUND) {
                    bmtLessB = std::make_shared<std::vector<BitwiseBmt>>(IntermediateDataSupport::pollBitwiseBmts(
                        BoolLessBatchOperator::bmtCount(xsBatches[b].size(), secrets[0]._width), 64));
                    bmtMuxB = std::make_shared<std::vector<BitwiseBmt>>(IntermediateDataSupport::pollBitwiseBmts(
                        2 * BoolMutexBatchOperator::bmtCount(xsBatches[b].size(), secrets[0]._width), 64));
                }

                futures.emplace_back(
                    ThreadPoolSupport::submit([&, b, bmtLessB, bmtMuxB]() -> BatchOutput {
                        auto &xsB = xsBatches[b];
                        auto &ysB = ysBatches[b];
                        auto &iB = xIdxBatches[b];
                        auto &jB = yIdxBatches[b];
                        auto &ascB = ascsBatches[b];
                        int sz = static_cast<int>(xsB.size());

                        int offset = std::max(BoolLessBatchOperator::tagStride(), BoolMutexBatchOperator::tagStride());

                        auto zs1 = BoolLessBatchOperator(
                            &xsB, &ysB,
                            secrets[0]._width,
                            taskTag, msgTagOffset + offset * b,
                            SecureOperator::NO_CLIENT_COMPUTE
                        ).setBmts(Conf::BMT_METHOD == Conf::BMT_BACKGROUND ? bmtLessB.get() : nullptr)->execute()->_zis;

                        for (int t = 0; t < sz; ++t) {
                            if (!ascB[t]) {
                                zs1[t] ^= Comm::rank();
                            }
                        }

                        auto zs2 = BoolMutexBatchOperator(
                            &xsB, &ysB, &zs1,
                            secrets[0]._width,
                            taskTag, msgTagOffset + offset * b
                        ).setBmts(Conf::BMT_METHOD == Conf::BMT_BACKGROUND ? bmtMuxB.get() : nullptr)->execute()->_zis;

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

void bitonicSortBool(std::vector<BoolSecret> &secrets, bool asc, int taskTag, int msgTagOffset) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        bitonicSortBoolSingleBatch(secrets, asc, taskTag, msgTagOffset);
    } else {
        bitonicSortBoolSplittedBatches(secrets, asc, taskTag, msgTagOffset);
    }
}

void doSort(std::vector<BoolSecret> &secrets, bool asc, int taskTag) {
    size_t n = secrets.size();
    bool isPowerOf2 = (n > 0) && ((n & (n - 1)) == 0);
    size_t paddingCount = 0;
    if (!isPowerOf2) {
        BoolSecret p;
        p._padding = true;

        size_t nextPow2 = static_cast<size_t>(1) <<
                          static_cast<size_t>(std::ceil(std::log2(n)));
        secrets.resize(nextPow2, p);
        paddingCount = nextPow2 - n;
    }
    bitonicSortBool(secrets, asc, taskTag, 0);
    if (paddingCount > 0) {
        secrets.resize(secrets.size() - paddingCount);
    }
}

void Secrets::sort(std::vector<BoolSecret> &secrets, bool asc, int taskTag) {
    doSort(secrets, asc, taskTag);
}

std::vector<int64_t> Secrets::boolShare(std::vector<int64_t> &origins, int clientRank, int width, int taskTag) {
    return BoolBatchOperator(origins, width, taskTag, 0, clientRank)._zis;
}

std::vector<int64_t> Secrets::boolReconstruct(std::vector<int64_t> &secrets, int clientRank, int width, int taskTag) {
    return BoolBatchOperator(secrets, width, taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).reconstruct(clientRank)->
            _results;
}

std::vector<int64_t> Secrets::arithShare(std::vector<int64_t> &origins, int clientRank, int width, int taskTag) {
    return ArithBatchOperator(origins, width, taskTag, 0, clientRank)._zis;
}

std::vector<int64_t> Secrets::arithReconstruct(std::vector<int64_t> &secrets, int clientRank, int width, int taskTag) {
    return ArithBatchOperator(secrets, width, taskTag, 0, SecureOperator::NO_CLIENT_COMPUTE).reconstruct(clientRank)->
            _results;
}

void bitonicSortArithSingleBatch(std::vector<ArithSecret> &secrets, bool asc, int taskTag, int msgTagOffset) {
    size_t n = secrets.size();
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            std::vector<int64_t> xs, ys;
            std::vector<int> xIdx, yIdx;
            std::vector<bool> ascs;
            size_t halfN = n / 2;
            xs.reserve(halfN);
            ys.reserve(halfN);
            xIdx.reserve(halfN);
            yIdx.reserve(halfN);
            ascs.reserve(halfN);

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
                xs.push_back(secrets[i]._data);
                xIdx.push_back(i);
                ys.push_back(secrets[l]._data);
                yIdx.push_back(l);
                ascs.push_back(dir ^ !asc);
            }

            auto zs = ArithLessBatchOperator(&xs, &ys, secrets[0]._width, taskTag, msgTagOffset,
                                             SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            int comparingCount = static_cast<int>(xs.size());
            for (int i = 0; i < comparingCount; i++) {
                if (!ascs[i]) {
                    zs[i] = zs[i] ^ Comm::rank();
                }
            }

            size_t concatSize = xs.size() + ys.size();
            xs.reserve(concatSize);
            ys.reserve(concatSize);
            zs.reserve(concatSize);
            auto bk = xs.end();
            xs.insert(xs.end(), ys.begin(), ys.end());
            ys.insert(ys.end(), xs.begin(), bk);
            zs.insert(zs.end(), zs.begin(), zs.end());
            zs = ArithMutexBatchOperator(&xs, &ys, &zs, secrets[0]._width, taskTag, msgTagOffset,
                                         SecureOperator::NO_CLIENT_COMPUTE).execute()->_zis;

            for (int i = 0; i < comparingCount; i++) {
                secrets[xIdx[i]]._data = zs[i];
                secrets[yIdx[i]]._data = zs[i + comparingCount];
            }
        }
    }
}

void bitonicSortArithSplittedBatches(std::vector<ArithSecret> &secrets, bool asc, int taskTag, int msgTagOffset) {
    int batchSize = Conf::BATCH_SIZE;
    size_t n = secrets.size();
    for (int k = 2; k <= n; k *= 2) {
        for (int j = k / 2; j > 0; j /= 2) {
            int comparingCount = 0;
            for (int i = 0; i < n; ++i) {
                int l = i ^ j;
                if (l <= i) {
                    continue;
                }
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

                        int offset = std::max(ArithLessBatchOperator::tagStride(secrets[0]._width),
                                              ArithMutexBatchOperator::tagStride(secrets[0]._width));

                        auto zs1 = ArithLessBatchOperator(
                            &xsB, &ysB,
                            secrets[0]._width,
                            taskTag, msgTagOffset + offset * b,
                            SecureOperator::NO_CLIENT_COMPUTE
                        ).execute()->_zis;

                        for (int t = 0; t < sz; ++t) {
                            if (!ascB[t]) {
                                zs1[t] ^= Comm::rank();
                            }
                        }

                        size_t concatSize = xsB.size() + ysB.size();
                        xsB.reserve(concatSize);
                        ysB.reserve(concatSize);
                        zs1.reserve(concatSize);
                        auto bk = xsB.end();
                        xsB.insert(xsB.end(), ysB.begin(), ysB.end());
                        ysB.insert(ysB.end(), xsB.begin(), bk);
                        zs1.insert(zs1.end(), zs1.begin(), zs1.end());
                        auto zs2 = ArithMutexBatchOperator(
                            &xsB, &ysB, &zs1,
                            secrets[0]._width,
                            taskTag, msgTagOffset + offset * b,
                            SecureOperator::NO_CLIENT_COMPUTE
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

void bitonicSortArith(std::vector<ArithSecret> &secrets, bool asc, int taskTag, int msgTagOffset) {
    if (Conf::BATCH_SIZE <= 0 || Conf::DISABLE_MULTI_THREAD) {
        bitonicSortArithSingleBatch(secrets, asc, taskTag, msgTagOffset);
    } else {
        bitonicSortArithSplittedBatches(secrets, asc, taskTag, msgTagOffset);
    }
}

void doSort(std::vector<ArithSecret> &secrets, bool asc, int taskTag) {
    size_t n = secrets.size();
    bool isPowerOf2 = (n > 0) && ((n & (n - 1)) == 0);
    size_t paddingCount = 0;
    if (!isPowerOf2) {
        ArithSecret p;
        p._padding = true;

        size_t nextPow2 = static_cast<size_t>(1) <<
                          static_cast<size_t>(std::ceil(std::log2(n)));
        secrets.resize(nextPow2, p);
        paddingCount = nextPow2 - n;
    }
    bitonicSortArith(secrets, asc, taskTag, 0);
    if (paddingCount > 0) {
        secrets.resize(secrets.size() - paddingCount);
    }
}

void Secrets::sort(std::vector<ArithSecret> &secrets, bool asc, int taskTag) {
    doSort(secrets, asc, taskTag);
}
