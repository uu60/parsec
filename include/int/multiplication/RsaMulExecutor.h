//
// Created by 杜建璋 on 2024/7/27.
//

#ifndef MPC_PACKAGE_RSAOTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_RSAOTMULTIPLICATIONSHAREEXECUTOR_H
#include "./AbstractMulExecutor.h"

template<typename T>
class RsaMulExecutor : public AbstractMulExecutor<T> {
public:
    RsaMulExecutor(T z, bool share);

    RsaMulExecutor(T x, T y, bool share);

protected:
    void obtainMultiplicationTriple() override;

    [[nodiscard]] std::string tag() const override;
};


#endif //MPC_PACKAGE_RSAOTMULTIPLICATIONSHAREEXECUTOR_H
