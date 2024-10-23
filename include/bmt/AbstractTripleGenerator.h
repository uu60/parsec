//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONTRIPLEGENERATOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONTRIPLEGENERATOR_H

#include "../SecureExecutor.h"

template<typename T>
class AbstractTripleGenerator : public SecureExecutor<T> {
protected:
    T _ai{};
    T _bi{};
    T _ci{};
    T _ui{};
    T _vi{};

public:
    [[nodiscard]] T ai() const;

    [[nodiscard]] T bi() const;

    [[nodiscard]] T ci() const;
};


#endif //MPC_PACKAGE_ABSTRACTMULTIPLICATIONTRIPLEGENERATOR_H
