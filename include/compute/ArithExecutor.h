//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include "../SecureExecutor.h"
#include <vector>
#include "../compute/BoolExecutor.h"
#include "../bmt/BMT.h"

class ArithExecutor : public SecureExecutor {
public:
    int64_t _xi{};
    int64_t _yi{};

    ArithExecutor(int64_t zi, int l, bool local);

    ArithExecutor(int64_t x, int64_t y, int l, bool local);

    ArithExecutor *reconstruct() override;

    [[nodiscard]] int64_t boolZi(std::vector<BMT> bmts) const;

    [[deprecated("This function should not be called.")]]
    ArithExecutor *execute() override;

    [[deprecated("This function should not be called.")]]
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_INTSHAREEXECUTOR_H
