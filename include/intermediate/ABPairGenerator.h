//
// Created by 杜建璋 on 2024/12/2.
//

#ifndef ABPAIRGENERATOR_H
#define ABPAIRGENERATOR_H
#include "./item/ABPair.h"
#include "../AbstractSecureExecutor.h"


class ABPairGenerator : public AbstractSecureExecutor {
private:
    explicit ABPairGenerator() : AbstractSecureExecutor(64, 1, 0) {
    }

public:
    ABPair _pair{};

    ABPairGenerator(const ABPairGenerator &) = delete;

    ABPairGenerator &operator=(const ABPairGenerator &) = delete;

    static ABPairGenerator &getInstance();

    ABPairGenerator *execute() override;

    ABPairGenerator *reconstruct(int clientRank) override;

protected:
    [[nodiscard]] std::string className() const override;
};


#endif //ABPAIRGENERATOR_H
