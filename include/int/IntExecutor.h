//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include "../SecureExecutor.h"

template<typename T>
class IntExecutor : public SecureExecutor<T> {
protected:
    T _xi{};
    T _yi{};

public:
    IntExecutor(T zi, bool share);

    IntExecutor(T x, T y, bool share);

    IntExecutor *reconstruct() override;

    // Do not use the following methods!
    IntExecutor *execute(bool reconstruct) override;

    [[nodiscard]] std::string tag() const override;

    IntExecutor *convertZiToBool();

    IntExecutor *convertZiToArithmetic();
};


#endif //MPC_PACKAGE_INTSHAREEXECUTOR_H
