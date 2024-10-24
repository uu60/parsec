//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H

#include "../../int/IntExecutor.h"

template<typename T>
class CompareExecutor : public IntExecutor<T> {
private:
    bool _sign{};
public:
    CompareExecutor(T z, bool share);

    CompareExecutor(T x, T y, bool share);

    CompareExecutor *execute(bool reconstruct) override;

    CompareExecutor *reconstruct() override;

    [[nodiscard]] std::string tag() const override;

    bool sign();
};

#endif //MPC_PACKAGE_COMPAREEXECUTOR_H
