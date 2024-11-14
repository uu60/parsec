//
// Created by 杜建璋 on 2024/11/12.
//

#include "IntAndExecutor.h"

template<class T>
IntAndExecutor<T> * IntAndExecutor<T>::execute(bool reconstruct) {

    return IntBoolExecutor<T>::execute(reconstruct);
}

template<class T>
IntAndExecutor<T> *IntAndExecutor<T>::obtainBmt(bool ai, bool bi, bool ci) {
    _ai = ai;
    _bi = bi;
    _ci = ci;
    return this;
}
