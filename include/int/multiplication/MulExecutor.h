//
// Created by 杜建璋 on 2024/7/13.
//

#ifndef MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#define MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
#include "../../int/IntArithExecutor.h"

template<typename T>
class MulExecutor : public IntArithExecutor<T> {
protected:
    T _ai{};
    T _bi{};
    T _ci{};

public:
    MulExecutor(T x, T y, bool share);

    MulExecutor *execute(bool reconstruct) override;

    MulExecutor *obtainBmt(T ai, T bi, T ci);

private:
    void process(bool reconstruct);
};


#endif //MPC_PACKAGE_ABSTRACTMULTIPLICATIONSHAREEXECUTOR_H
