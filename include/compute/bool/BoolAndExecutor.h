//
// Created by 杜建璋 on 2024/11/12.
//

#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "../BoolExecutor.h"

class BoolAndExecutor : public BoolExecutor {
public:
    static int _totalMsgTagNum;
    static int32_t _currentObjTag;

    BoolAndExecutor(bool x, bool y, int l, int32_t objTag, int8_t msgTagOffset,
                       int clientRank) : BoolExecutor(x, y, l, objTag, msgTagOffset, clientRank) {
    };

    BoolAndExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int8_t msgNum(int l);
};


#endif //INTANDEXECUTOR_H
