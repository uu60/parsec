//
// Created by 杜建璋 on 2024/11/7.
//

#ifndef INTBOOLEXECUTOR_H
#define INTBOOLEXECUTOR_H
#include "../SecureExecutor.h"

template <class T>
class IntBoolExecutor : public SecureExecutor<T> {
public:
    T _xi{};
    T _yi{};

    IntBoolExecutor(T z, bool share);

    IntBoolExecutor(T x, T y, bool share);

    IntBoolExecutor *reconstruct() override;

    T convertZiToArithmetic(bool ot);

    IntBoolExecutor *execute(bool reconstruct) override;

    // DO NOT USE
    [[nodiscard]] std::string tag() const override;

private:
    void doOtConvert();

    void doPregeneratedConvert();

    std::pair<T, T> getPair(int idx);
};

#endif //INTBOOLEXECUTOR_H
