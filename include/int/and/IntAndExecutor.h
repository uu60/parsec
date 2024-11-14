//
// Created by 杜建璋 on 2024/11/12.
//

#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "../IntBoolExecutor.h"

template<class T>
class IntAndExecutor : public IntBoolExecutor<T> {
protected:
    // triple
    T _ai{};
    T _bi{};
    T _ci{};

public:
    IntAndExecutor(bool x, bool y) : IntBoolExecutor<T>(x, y) {
    };

    IntAndExecutor(bool x, bool y, bool share) : IntBoolExecutor<T>(x, y, share) {
    };

    IntAndExecutor *execute(bool reconstruct) override;

    IntAndExecutor *obtainBmt(bool ai, bool bi, bool ci);
};


#endif //INTANDEXECUTOR_H
