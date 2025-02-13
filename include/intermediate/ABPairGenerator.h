//
// Created by 杜建璋 on 2024/12/2.
//

#ifndef ABPAIRGENERATOR_H
#define ABPAIRGENERATOR_H
#include "./item/ABPair.h"
#include "../base/AbstractSingleExecutor.h"


class ABPairGenerator : public AbstractSingleExecutor {
private:
    explicit ABPairGenerator() : AbstractSingleExecutor(64, 1, 0) {
    }

public:
    ABPair _pair{};

    ABPairGenerator(const ABPairGenerator &) = delete;

    ABPairGenerator &operator=(const ABPairGenerator &) = delete;

    static ABPairGenerator &getInstance();

    ABPairGenerator *execute() override;

    ABPairGenerator *reconstruct(int clientRank) override;
};


#endif //ABPAIRGENERATOR_H
