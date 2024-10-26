//
// Created by 杜建璋 on 2024/8/30.
//

#include "bmt/FixedTripleGenerator.h"

#if __has_include("bmt/fixed_triples_0.h")

#include "bmt/fixed_triples_0.h"

#endif

#if __has_include("bmt/fixed_triples_1.h")

#include "bmt/fixed_triples_1.h"

#endif

#include "utils/Comm.h"
#include "utils/Math.h"

template<typename T>
FixedTripleGenerator<T>::FixedTripleGenerator() = default;

template<typename T>
FixedTripleGenerator<T> *FixedTripleGenerator<T>::execute(bool dummy) {
    constexpr int length = std::is_same_v<T, bool> ? 8 : 100;
    int idx = 0;
    if (Comm::rank() == 0) {
        idx = (int) Math::randInt(0, length - 1);
        Comm::ssend(&idx);
    } else {
        Comm::srecv(&idx);
    }
    std::tuple<T, T, T> triple = getTriple(idx);
    this->_ai = std::get<0>(triple);
    this->_bi = std::get<1>(triple);
    this->_ci = std::get<2>(triple);
    return this;
}

template<typename T>
std::string FixedTripleGenerator<T>::tag() const {
    return "[Fixed BMT Generator]";
}

template<typename T>
std::tuple<T, T, T>
FixedTripleGenerator<T>::getTriple(int idx) {
    constexpr int length = std::is_same_v<T, bool> ? 8 : 100;
    const std::array<std::tuple<T, T, T>, length> *triple;
    if constexpr (std::is_same_v<T, bool>) {
        triple = &TRIPLES_1;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        triple = &TRIPLES_8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        triple = &TRIPLES_16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        triple = &TRIPLES_32;
    } else {
        triple = &TRIPLES_64;
    }
    return (*triple)[idx];
}

template
class FixedTripleGenerator<bool>;

template
class FixedTripleGenerator<int8_t>;

template
class FixedTripleGenerator<int16_t>;

template
class FixedTripleGenerator<int32_t>;

template
class FixedTripleGenerator<int64_t>;