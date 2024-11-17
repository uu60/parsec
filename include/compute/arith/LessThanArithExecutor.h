//
// Created by 杜建璋 on 2024/9/2.
//

#ifndef MPC_PACKAGE_COMPAREEXECUTOR_H
#define MPC_PACKAGE_COMPAREEXECUTOR_H
#include "../ArithExecutor.h"
#include <vector>

class LessThanArithExecutor : public ArithExecutor {
public:
    bool _sign{};
    std::vector<BMT> _bmts{};

    LessThanArithExecutor(int64_t z, int l, bool local);

    LessThanArithExecutor(int64_t x, int64_t y, int l, bool local);

    /**
     * Provide BMTs for less than comparison.
     * @param bmts a vector with 3 * _l BMTs
     * @return itself
     */
    LessThanArithExecutor *setBmts(std::vector<BMT> bmts);

    LessThanArithExecutor *execute() override;

    LessThanArithExecutor *reconstruct() override;

    [[nodiscard]] std::string tag() const override;
};

#endif //MPC_PACKAGE_COMPAREEXECUTOR_H
