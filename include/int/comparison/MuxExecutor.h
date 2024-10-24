//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "../../int/IntExecutor.h"

template<typename T>
class MuxExecutor : public IntExecutor<T> {
private:
    T _ci{};

public:
    MuxExecutor(T x, T y, bool c);

    MuxExecutor(T xi, T yi, T ci);

    MuxExecutor *execute(bool reconstruct) override;

    [[nodiscard]] string tag() const override;
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
