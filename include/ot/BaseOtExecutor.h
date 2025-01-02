//
// Created by 杜建璋 on 2024/7/15.
//

#ifndef MPC_PACKAGE_RSAOTEXECUTOR_H
#define MPC_PACKAGE_RSAOTEXECUTOR_H

#include <string>

#include "AbstractOtExecutor.h"
#include "../AbstractSecureExecutor.h"

// according to https://blog.csdn.net/qq_16763983/article/details/128055146
class BaseOtExecutor : public AbstractOtExecutor {
private:
    // RSA key _bits
    int _bits{};

    // params for sender
    std::string _rand0{};
    std::string _rand1{};
    std::string _pri;

    // params for receiver
    std::string _randK{};

    // params for both
    std::string _pub;

public:
    // _m0 and _m1 are for sender (invalid for receiver)
    // i is for receiver (invalid for sender)
    explicit BaseOtExecutor(int sender, int64_t m0, int64_t m1, int choice, int l, int16_t taskTag, int16_t msgTagOffset);

    explicit BaseOtExecutor(int bits, int sender, int64_t m0, int64_t m1, int choice, int l, int16_t taskTag, int16_t msgTagOffset);

    BaseOtExecutor *execute() override;

    [[nodiscard]] static int16_t needsMsgTags();

protected:
    [[nodiscard]] std::string className() const override;

private:
    // methods for sender
    void generateAndShareRsaKeys();

    void generateAndShareRandoms();

    // methods for both
    void process();
};


#endif //MPC_PACKAGE_RSAOTEXECUTOR_H
