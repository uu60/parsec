//
// Created by 杜建璋 on 2024/8/30.
//

#include "bmt/AbstractTripleGenerator.h"

template<typename T>
T AbstractTripleGenerator<T>::ai() const {
    return _ai;
}

template<typename T>
T AbstractTripleGenerator<T>::bi() const {
    return _bi;
}

template<typename T>
T AbstractTripleGenerator<T>::ci() const {
    return _ci;
}

template class AbstractTripleGenerator<bool>;
template class AbstractTripleGenerator<int8_t>;
template class AbstractTripleGenerator<int16_t>;
template class AbstractTripleGenerator<int32_t>;
template class AbstractTripleGenerator<int64_t>;