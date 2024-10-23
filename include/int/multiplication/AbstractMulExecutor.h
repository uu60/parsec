//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#include "../../int/IntExecutor.h"

template<typename T>
class AbstractMulExecutor : public IntExecutor<T> {
protected:
    T _ai{};
    T _bi{};
    T _ci{};

public:
    AbstractMulExecutor(T z, bool share);

    AbstractMulExecutor(T x, T y, bool share);

    AbstractMulExecutor *execute(bool reconstruct) override;

protected:
    virtual void obtainMultiplicationTriple() = 0;

private:
    void process(bool reconstruct);
};


#endif //MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
