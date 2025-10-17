
#ifndef MPC_PACKAGE_RSAOTEXECUTOR_H
#define MPC_PACKAGE_RSAOTEXECUTOR_H

#include <string>

#include "./base/AbstractOtOperator.h"

class BaseOtOperator : public AbstractOtOperator {
private:
    int _bits{};

    std::string _rand0{};
    std::string _rand1{};
    std::string _pri;

    std::string _randK{};

    std::string _pub;

public:
    explicit BaseOtOperator(int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset);

    explicit BaseOtOperator(int bits, int sender, int64_t m0, int64_t m1, int choice, int l, int taskTag, int msgTagOffset);

    BaseOtOperator *execute() override;

    [[nodiscard]] static int tagStride();

private:
    void generateAndShareRsaKeys();

    void generateAndShareRandoms();

    void process();
};


#endif
