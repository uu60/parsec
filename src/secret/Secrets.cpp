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
void bitonicSort(std::vector<SecretT> &secrets, bool asc, int taskTag, int msgTagOffset) {
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
                int l = i ^ j; // CAS secrets[i] and secrets[l]
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
