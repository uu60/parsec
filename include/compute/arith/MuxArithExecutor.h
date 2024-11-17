//
// Created by 杜建璋 on 2024/10/23.
//

#ifndef MPC_PACKAGE_MUXEXECUTOR_H
#define MPC_PACKAGE_MUXEXECUTOR_H
#include "../ArithExecutor.h"
#include "../../bmt/BMT.h"

class MuxArithExecutor : public ArithExecutor {
private:
    bool _cond_i{};

    // 2 triples needed
    BMT _bmt0;
    BMT _bmt1;

public:
    MuxArithExecutor(int64_t x, int64_t y, int l, bool c, bool local);

    MuxArithExecutor *execute() override;

    [[nodiscard]] string tag() const override;

    MuxArithExecutor *setBmts(BMT bmt0, BMT bmt1);
};

#endif //MPC_PACKAGE_MUXEXECUTOR_H
