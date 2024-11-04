//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H

#include "../../int/IntExecutor.h"

template<typename T>
class CompareExecutor : public IntExecutor<T> {
public:
    bool _sign{};

    CompareExecutor(T z, bool share);

    CompareExecutor(T x, T y, bool share);

    CompareExecutor *execute(bool reconstruct) override;

    CompareExecutor *reconstruct() override;

    [[nodiscard]] std::string tag() const override;
};

#endif //MPC_PACKAGE_COMPAREEXECUTOR_H
