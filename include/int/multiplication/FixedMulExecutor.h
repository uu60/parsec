//
// Created by 杜建璋 on 2024/7/27.
//

#ifndef DEMO_FIXEDMULTIPLICATIONSHAREEXECUTOR_H
#define DEMO_FIXEDMULTIPLICATIONSHAREEXECUTOR_H
#include "./AbstractMulExecutor.h"

template<typename T>
class FixedMulExecutor : public AbstractMulExecutor<T> {
public:
    FixedMulExecutor(T z, bool share);
    FixedMulExecutor(T x, T y, bool share);

protected:
    [[nodiscard]] std::string tag() const override;
    void obtainMultiplicationTriple() override;
};


#endif //DEMO_FIXEDMULTIPLICATIONSHAREEXECUTOR_H
