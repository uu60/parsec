//
// Created by 杜建璋 on 2024/9/6.
//

#ifndef MPC_PACKAGE_INTSHAREEXECUTOR_H
#define MPC_PACKAGE_INTSHAREEXECUTOR_H

#include "../SecureExecutor.h"
#include <utility>

template<typename T>
class IntExecutor : public SecureExecutor<T> {
protected:
    T _xi{};
    T _yi{};

public:
    IntExecutor(T zi, bool share);

    IntExecutor(T x, T y, bool share);

    IntExecutor *reconstruct() override;

    IntExecutor *convertZiToBool();

    IntExecutor *convertZiToArithmetic(bool ot);

    // DO NOT USE
    IntExecutor *execute(bool reconstruct) override;

    // DO NOT USE
    [[nodiscard]] std::string tag() const override;

    T xi();

    T yi();

private:
    void doConvertByOt();

    void doConvertByFixedRand();

    std::pair<T, T> getPair(int idx);
};


#endif //MPC_PACKAGE_INTSHAREEXECUTOR_H
