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
public:
    explicit FixedTripleGenerator();

    FixedTripleGenerator *execute(bool dummy) override;

protected:
    [[nodiscard]] std::string tag() const override;

private:
    std::tuple<T, T, T> getTriple(int idx);
};


#endif //MPC_PACKAGE_FIXEDTRIPLEGENERATOR_H
