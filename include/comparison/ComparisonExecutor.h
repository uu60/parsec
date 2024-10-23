//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPARISONEXECUTOR_H
#define MPC_PACKAGE_COMPARISONEXECUTOR_H

#include "../int/IntExecutor.h"

template<typename T>
class ComparisonExecutor : public IntExecutor<T> {
private:
    bool _sign{};
public:
    ComparisonExecutor(T z, bool share);

    ComparisonExecutor(T x, T y, bool share);

    ComparisonExecutor *execute(bool reconstruct) override;

    ComparisonExecutor *reconstruct() override;

    [[nodiscard]] std::string tag() const override;

    bool sign();
};

#endif //MPC_PACKAGE_COMPARISONEXECUTOR_H
