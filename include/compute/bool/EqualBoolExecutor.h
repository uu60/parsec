//
// Created by 杜建璋 on 2024/11/15.
//

#ifndef BOOLEQUALEXECUTOR_H
#define BOOLEQUALEXECUTOR_H
#
#include "compute/BoolExecutor.h"

class EqualBoolExecutor : public BoolExecutor {
public:
    bool _sign{};

    EqualBoolExecutor(int64_t z, int l, bool local) : BoolExecutor(z, l, local) {};

    EqualBoolExecutor(int64_t x, int64_t y, int l, bool local) : BoolExecutor(x, y, l, local) {};

    EqualBoolExecutor *execute() override;

    EqualBoolExecutor *reconstruct() override;

    [[nodiscard]] std::string tag() const override;
};

#endif //BOOLEQUALEXECUTOR_H
