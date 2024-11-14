//
// Created by 杜建璋 on 2024/9/12.
//

#include "api/IntSecret.h"
#include "int/IntArithExecutor.h"
#include "int/IntBoolExecutor.h"
#include "int/addition/AddExecutor.h"
#include "int/multiplication/MulExecutor.h"
#include "int/comparison/ArithLessThanExecutor.h"
#include "int/comparison/MuxExecutor.h"

template<typename T>
IntSecret<T>::IntSecret(T x) {
    _data = x;
}

template<typename T>
IntSecret<T> IntSecret<T>::arithShare() const {
    return IntSecret(IntArithExecutor<T>(_data, true)._zi);
}

template<typename T>
IntSecret<T> IntSecret<T>::boolShare() const {
    return IntSecret(IntBoolExecutor<T>(_data, true)._zi);
}

template<typename T>
[[nodiscard]] IntSecret<T> IntSecret<T>::arithReconstruct() const {
    return IntSecret(IntArithExecutor<T>(_data, false).reconstruct()->_result);
}

template<typename T>
[[nodiscard]] IntSecret<T> IntSecret<T>::boolReconstruct() const {
    return IntSecret(IntBoolExecutor<T>(_data, false).reconstruct()->_result);
}

template<typename T>
T IntSecret<T>::get() const {
    return _data;
}

template<typename T>
IntSecret<T> IntSecret<T>::add(IntSecret yi) const {
    return IntSecret(AddExecutor<T>(_data, yi.get(), false).execute(false)->_zi);
}

template<typename T>
IntSecret<T> IntSecret<T>::mul(IntSecret yi, T ai, T bi, T ci) const {
    return IntSecret(MulExecutor<T>(_data, yi.get(), false).obtainBmt(ai, bi, ci)->execute(false)->_zi);
}

template<typename T>
IntSecret<T> IntSecret<T>::boolean() const {
    return IntSecret(IntArithExecutor<T>(_data, false).convertZiToBool());
}

template<typename T>
IntSecret<T> IntSecret<T>::arithmetic() const {
    return IntSecret(IntBoolExecutor<T>(_data, false).convertZiToArithmetic(true));
}

template<typename T>
BitSecret IntSecret<T>::arithLessThan(IntSecret yi) const {
    return BitSecret(ArithLessThanExecutor<T>(_data, yi.get(), false).execute(false)->_sign);
}

template<typename T>
IntSecret<T> IntSecret<T>::mux(IntSecret yi, BitSecret cond_i, T ai, T bi, T ci) const {
    return IntSecret(MuxExecutor<T>(_data, yi.get(), cond_i.get(), false).obtainBmt(ai, bi, ci)->execute(false)->_zi);
}

template
class IntSecret<int8_t>;

template
class IntSecret<int16_t>;

template
class IntSecret<int32_t>;

template
class IntSecret<int64_t>;
