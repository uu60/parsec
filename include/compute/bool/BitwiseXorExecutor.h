//
// Created by 杜建璋 on 2024/11/15.
//

#ifndef XOREXECUTOR_H
#define XOREXECUTOR_H
#include "../BoolExecutor.h"

class BitwiseXorExecutor : public BoolExecutor {
public:
    BitwiseXorExecutor(int64_t z, int l, bool local) : BoolExecutor(z, l, local) {
    };

    BitwiseXorExecutor(bool x, bool y, int l, bool local) : BoolExecutor(x, y, l, local) {
    };

    BitwiseXorExecutor *execute() override;
};



#endif //XOREXECUTOR_H
