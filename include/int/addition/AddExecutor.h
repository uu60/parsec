//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H

#include "../../int/IntArithExecutor.h"
#include <vector>

template<typename T>
class AddExecutor : public IntArithExecutor<T> {
public:
    AddExecutor(T x, T y, bool share);

    AddExecutor *execute(bool reconstruct) override;

protected:
    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_ADDITIONSHAREEXECUTOR_H
