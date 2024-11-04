//
// Created by 杜建璋 on 2024/8/30.
//

#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONTRIPLEGENERATOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONTRIPLEGENERATOR_H

#include "../SecureExecutor.h"

template<typename T>
class AbstractTripleGenerator : public SecureExecutor<T> {
public:
    T _ai{};
    T _bi{};
    T _ci{};
    T _ui{};
    T _vi{};
};


#endif //MPC_PACKAGE_ABSTRACTMULTIPLICATIONTRIPLEGENERATOR_H
