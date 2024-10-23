//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_FIXEDTRIPLEGENERATOR_H
#define MPC_PACKAGE_FIXEDTRIPLEGENERATOR_H
#include "./AbstractTripleGenerator.h"
#include <array>
#include <utility>  // For std::pair
#include <tuple>   // For std::tuple

template<typename T>
class FixedTripleGenerator : public AbstractTripleGenerator<T> {
private:
    std::tuple<std::pair<T, T>, std::pair<T, T>, std::pair<T, T>> getRandomTriple(int idx);
public:
    explicit FixedTripleGenerator();
    FixedTripleGenerator* execute(bool dummy) override;
protected:
    [[nodiscard]] std::string tag() const override;
// fixed MTs
private:
    static const std::array<std::tuple<std::pair<bool, bool>, std::pair<bool, bool>, std::pair<bool,
            bool>>, 100> TRIPLES_1;

    static const std::array<std::tuple<std::pair<int8_t, int8_t>, std::pair<int8_t, int8_t>, std::pair<int8_t, int8_t>>, 100> TRIPLES_8;

    static const std::array<std::tuple<std::pair<int16_t, int16_t>, std::pair<int16_t, int16_t>, std::pair<int16_t, int16_t>>, 100> TRIPLES_16;

    static const std::array<std::tuple<std::pair<int32_t, int32_t>, std::pair<int32_t, int32_t>, std::pair<int32_t, int32_t>>, 100> TRIPLES_32;

    static const std::array<std::tuple<std::pair<int64_t, int64_t>, std::pair<int64_t, int64_t>, std::pair<int64_t, int64_t>>, 100> TRIPLES_64;
};


#endif //MPC_PACKAGE_FIXEDTRIPLEGENERATOR_H
