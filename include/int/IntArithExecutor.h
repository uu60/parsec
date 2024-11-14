//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include "../SecureExecutor.h"
#include "../int/IntBoolExecutor.h"

template<typename T>
class IntArithExecutor : public SecureExecutor<T> {
public:
    T _xi{};
    T _yi{};

public:
    IntArithExecutor(T zi, bool share);

    IntArithExecutor(T x, T y, bool share);

    IntArithExecutor *reconstruct() override;

    T convertZiToBool();

    // DO NOT USE
    IntArithExecutor *execute(bool reconstruct) override;

    // DO NOT USE
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_INTSHAREEXECUTOR_H
