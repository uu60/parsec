//
// Created by 杜建璋 on 2024/11/7.
//

#ifndef INTBOOLEXECUTOR_H
#define INTBOOLEXECUTOR_H
#include "../SecureExecutor.h"

class BoolExecutor : public SecureExecutor {
public:
    int64_t _xi{};
    int64_t _yi{};

    BoolExecutor(int64_t z, int l, bool local);

    BoolExecutor(int64_t x, int64_t y, int l, bool local);

    BoolExecutor *reconstruct() override;

    [[nodiscard]] int64_t arithZi() const;

    BoolExecutor *execute() override;

    // DO NOint64_t USE
    [[nodiscard]] std::string tag() const override;

// private:
//     void doOtConvert();
//
//     void doPregeneratedConvert();
//
//     std::pair<T, T> getPair(compute idx);
};

#endif //INTBOOLEXECUTOR_H
