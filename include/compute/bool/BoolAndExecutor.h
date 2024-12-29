//
// Created by 杜建璋 on 2024/11/12.
//

#ifndef INTANDEXECUTOR_H
#define INTANDEXECUTOR_H
#include "./BoolExecutor.h"
#include "../../intermediate/item/Bmt.h"

class BoolAndExecutor : public BoolExecutor {
private:
    std::vector<Bmt> *_bmts{};

public:
    BoolAndExecutor(bool x, bool y, int l, int16_t objTag, int16_t msgTagOffset,
                       int clientRank) : BoolExecutor(x, y, l, objTag, msgTagOffset, clientRank) {
    };

    BoolAndExecutor *execute() override;

    [[nodiscard]] std::string className() const override;

    [[nodiscard]] static int16_t neededMsgTags(int l);

    BoolAndExecutor *setBmts(std::vector<Bmt> *bmts);
};


#endif //INTANDEXECUTOR_H
