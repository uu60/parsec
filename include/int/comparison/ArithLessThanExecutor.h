//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H

#include "../../int/IntArithExecutor.h"

template<typename T>
class ArithLessThanExecutor : public IntArithExecutor<T> {
public:
    bool _sign{};

    ArithLessThanExecutor(T z, bool share);

    ArithLessThanExecutor(T x, T y, bool share);

    ArithLessThanExecutor *execute(bool reconstruct) override;

//    ArithLessThanExecutor *boolEqual();

    ArithLessThanExecutor *reconstruct() override;

    [[nodiscard]] std::string tag() const override;
};

#endif //MPC_PACKAGE_COMPAREEXECUTOR_H
