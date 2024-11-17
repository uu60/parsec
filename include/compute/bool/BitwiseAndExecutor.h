//
// Created by 杜建璋 on 2024/11/12.
//

#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "../BoolExecutor.h"
#include "../../bmt/BMT.h"
#include <vector>

class BitwiseAndExecutor : public BoolExecutor {
protected:
    // triple
    std::vector<BMT> _bmts{};

public:
    BitwiseAndExecutor(int64_t z, int l, bool local) : BoolExecutor(z, l, local) {
    };

    BitwiseAndExecutor(bool x, bool y, int l, bool local) : BoolExecutor(x, y, l, local) {
    };

    BitwiseAndExecutor *execute() override;

    BitwiseAndExecutor *setBmts(std::vector<BMT> bmts);
};


#endif //INTANDEXECUTOR_H
