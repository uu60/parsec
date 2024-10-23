//
// Created by 杜建璋 on 2024/9/12.
//

#include "api/IntSecret.h"
#include "int/IntExecutor.h"
#include "int/multiplication/RsaMulExecutor.h"
#include "comparison/ComparisonExecutor.h"

template<typename T>
IntSecret<T>::IntSecret(T x) {
    _data = x;
}

template<typename T>
IntSecret<T> IntSecret<T>::add(T yi) const {
    return IntSecret(_data + yi);
}

template<typename T>
IntSecret<T> IntSecret<T>::add(T xi, T yi) {
    return IntSecret(xi + yi);
}

template<typename T>
IntSecret<T> IntSecret<T>::mul(T yi) const {
    return IntSecret(RsaMulExecutor(_data, yi, false).execute(false)->result());
}

template<typename T>
IntSecret<T> IntSecret<T>::share() const {
    return IntSecret(IntExecutor<T>(_data, true).zi());
}

template<typename T>
IntSecret<T> IntSecret<T>::reconstruct() const {
    return IntSecret(IntExecutor<T>(_data, false).reconstruct()->result());
}

template<typename T>
IntSecret<T> IntSecret<T>::share(T x) {
    return IntSecret(IntExecutor<T>(x, true).zi());
}

template<typename T>
IntSecret<T> IntSecret<T>::mul(T xi, T yi) {
    return IntSecret(RsaMulExecutor<T>(xi, yi, false).execute(false)->result());
}

template<typename T>
T IntSecret<T>::get() const {
    return _data;
}

template<typename T>
IntSecret<T> IntSecret<T>::sum(const std::vector<T> &xis) {
    IntSecret ret(0);
    for (T x: xis) {
        ret = ret.add(x);
    }
    return ret;
}

template<typename T>
IntSecret<T> IntSecret<T>::add(IntSecret yi) const {
    return add(yi.get());
}

template<typename T>
IntSecret<T> IntSecret<T>::mul(IntSecret yi) const {
    return mul(yi.get());
}

template<typename T>
IntSecret<T> IntSecret<T>::sum(const std::vector<T> &xis, const std::vector<T> &yis) {
    IntSecret<T> ret(0);
    for (int i = 0; i < xis.size(); i++) {
        ret = ret.add(xis[i]).add(yis[i]);
    }
    return ret;
}

template<typename T>
IntSecret<T> IntSecret<T>::share(IntSecret<T> x) {
    return share(x.get());
}


template<typename T>
IntSecret<T> IntSecret<T>::add(IntSecret<T> xi, IntSecret<T> yi) {
    return add(xi.get(), yi.get());
}

template<typename T>
IntSecret<T> IntSecret<T>::mul(IntSecret<T> xi, IntSecret<T> yi) {
    return mul(xi.get(), yi.get());
}

template<typename T>
IntSecret<T> IntSecret<T>::sum(const std::vector<IntSecret<T> > &xis) {
    std::vector<T> temp(xis.size());
    for (IntSecret x: xis) {
        temp.push_back(x.get());
    }
    return sum(temp);
}

template<typename T>
IntSecret<T> IntSecret<T>::sum(const std::vector<IntSecret<T> > &xis, const std::vector<IntSecret<T> > &yis) {
    std::vector<T> xs(xis.size());
    for (IntSecret x: xis) {
        xs.push_back(x.get());
    }
    std::vector<T> ys(yis.size());
    for (IntSecret y: yis) {
        ys.push_back(y.get());
    }
    return sum(xs, ys);
}


template<typename T>
IntSecret<T> IntSecret<T>::product(const std::vector<T> &xis) {
    IntSecret ret(xis[0]);
    for (int i = 0; i < xis.size() - 1; i++) {
        ret = ret.mul(xis[i + 1]);
    }
    return ret;
}

template<typename T>
IntSecret<T> IntSecret<T>::product(const std::vector<IntSecret> &xis) {
    std::vector<T> vals(xis.size());
    for (IntSecret x: xis) {
        vals.push_back(x.get());
    }
    return product(vals);
}

template<typename T>
IntSecret<T> IntSecret<T>::dot(const std::vector<T> &xis, const std::vector<T> &yis) {
    IntSecret ret(0);
    for (int i = 0; i < xis.size() - 1; i++) {
        ret = ret.add(IntSecret(xis[i]).mul(yis[i]));
    }
    return ret;
}

template<typename T>
IntSecret<T> IntSecret<T>::convertToBool() const {
    return IntSecret(IntExecutor<T>(_data, false).convertZiToBool()->zi());
}

template<typename T>
IntSecret<T> IntSecret<T>::convertToArithmetic() const {
    return IntSecret(IntExecutor<T>(_data, false).convertZiToArithmetic()->zi());
}

template<typename T>
BitSecret IntSecret<T>::compare(T yi) const {
    return BitSecret(ComparisonExecutor<T>(_data, yi, false).execute(false)->sign());
}

template<typename T>
BitSecret IntSecret<T>::compare(IntSecret yi) const {
    return compare(yi.get());
}

template
class IntSecret<int8_t>;

template
class IntSecret<int16_t>;

template
class IntSecret<int32_t>;

template
class IntSecret<int64_t>;
