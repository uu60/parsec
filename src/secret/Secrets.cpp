//
// Created by 杜建璋 on 2025/2/12.
//

#include "secret/Secrets.h"
#include "utils/System.h"
#include <cmath>

template<typename SecretT>
void compareAndSwap(std::vector<SecretT> &secrets, size_t i, size_t j, bool dir) {
    if (secrets[i]._padding && secrets[j]._padding) {
        return;
    }
    if ((secrets[i]._padding && dir) || (secrets[j]._padding && !dir)) {
        std::swap(secrets[i], secrets[j]);
        return;
    }
    if (secrets[i]._padding || secrets[j]._padding) {
        return;
    }
    auto swap = secrets[i].lessThan(secrets[j]).not_();
    if (!dir) {
        swap = swap.not_();
    }
    auto tempI = secrets[j].mux(secrets[i], swap);
    auto tempJ = secrets[i].mux(secrets[j], swap);
    secrets[i] = tempI;
    secrets[j] = tempJ;
}

template<typename SecretT>
void bitonicMerge(std::vector<SecretT> &secrets, size_t low, size_t length, bool dir) {
    if (length > 1) {
        size_t mid = length / 2;
        for (size_t i = low; i < low + mid; i++) {
            compareAndSwap<SecretT>(secrets, i, i + mid, dir);
        }
        bitonicMerge<SecretT>(secrets, low, mid, dir);
        bitonicMerge<SecretT>(secrets, low + mid, mid, dir);
    }
}

template<typename SecretT>
void bitonicSort(std::vector<SecretT> &secrets, size_t low, size_t length, bool dir) {
    if (length > 1) {
        size_t mid = length / 2;
        bitonicSort<SecretT>(secrets, low, mid, true);
        bitonicSort<SecretT>(secrets, low + mid, mid, false);
        bitonicMerge<SecretT>(secrets, low, length, dir);
    }
}

template<typename SecretT>
void doSort(std::vector<SecretT> &secrets, bool asc) {
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
    bitonicSort<SecretT>(secrets, 0, secrets.size(), asc);
    if (paddingCount > 0) {
        secrets.resize(secrets.size() - paddingCount);
    }
}

void Secrets::sort(std::vector<ArithSecret> &secrets, bool asc) {
    doSort<ArithSecret>(secrets, asc);
}

void Secrets::sort(std::vector<BoolSecret> &secrets, bool asc) {
    doSort<BoolSecret>(secrets, asc);
}
