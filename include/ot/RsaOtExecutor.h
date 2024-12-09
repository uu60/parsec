//
// Created by 杜建璋 on 2024/7/15.
//

#ifndef MPC_PACKAGE_RSAOTEXECUTOR_H
#define MPC_PACKAGE_RSAOTEXECUTOR_H

#include <string>
#include "../AbstractSecureExecutor.h"

// according to https://blog.csdn.net/qq_16763983/article/details/128055146
class RsaOtExecutor : public AbstractSecureExecutor {
public:
    // RSA key _bits
    int _bits{};
    // correspond mpi rank
    bool _isSender{};
    bool _isReceiver{};
    // params for sender
    std::string _rand0{};
    std::string _rand1{};
    std::string _pri;
    int64_t _m0{};
    int64_t _m1{};

    // params for receiver
    std::string _randK{};
    int _i; // msg choice

    // params for both
    std::string _pub;

public:
    // _m0 and _m1 are for sender (invalid for receiver)
    // i is for receiver (invalid for sender)
    explicit RsaOtExecutor(int sender, int receiver, int64_t m0, int64_t m1, int i, int l, int32_t objTag, int8_t msgTagOffset);

    explicit RsaOtExecutor(int bits, int sender, int receiver, int64_t m0, int64_t m1, int i, int l, int32_t objTag, int8_t msgTagOffset);

    RsaOtExecutor *execute() override;

    RsaOtExecutor *reconstruct(int clientRank) override;

    [[nodiscard]] static int8_t msgNum();

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
