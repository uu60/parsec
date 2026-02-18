
#ifndef MPC_PACKAGE_RSAOTEXECUTOR_H
#define MPC_PACKAGE_RSAOTEXECUTOR_H

#include <string>

#include "./base/AbstractOtOperator.h"

class BaseOtOperator : public AbstractOtOperator {
private:
    std::string _rand0{};
    std::string _rand1{};
    std::string _randK{};

public:
    explicit BaseOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset);


    BaseOtOperator *execute() override;

    [[nodiscard]] static int tagStride();

private:

    void generateAndShareRandoms();

    void process();
};


#endif
