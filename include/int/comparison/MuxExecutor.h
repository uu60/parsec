//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "../../SecureExecutor.h"

template<typename T>
class MuxExecutor : public SecureExecutor<T> {
private:
    T _xi{};
    T _yi{};
    bool _cond_i{};

    T _ai{};
    T _bi{};
    T _ci{};

public:
    MuxExecutor(T x, T y, bool c, bool share);

    MuxExecutor *execute(bool reconstruct) override;

    [[nodiscard]] string tag() const override;

    SecureExecutor<T> * reconstruct() override;

    MuxExecutor *obtainBmt(T ai, T bi, T ci);
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
