//
// Created by 杜建璋 on 2024/7/15.
//

#ifndef MPC_PACKAGE_COMM_H
#define MPC_PACKAGE_COMM_H
#include "./IComm.h"

/**
 * For sender, mpi rank must be 0 or 1.
 * The task publisher must be rank of 2.
 * Attention: Currently, there is no restriction in this util class.
 */
class MpiComm : public IComm {
public:
    static const int CLIENT_RANK;

private:
    // joined party number
    int _mpiSize{};
    // _mpiRank of current device
    int _mpiRank{};

public:
    int rank() override;

    // init env
    void init(int argc, char **argv) override;

    void finalize() override;

    // judge identity
    bool isServer() override;

    bool isClient() override;

    // exchange source (for rank of 0 and 1)
    void serverExchange(const int64_t *source, int64_t *target, int tag) override;

    void serverExchange(const bool *source, bool *target, int tag) override;

    // serverSend
    void serverSend(const int64_t *source, int tag) override;

    void serverSend(const bool *source, int tag) override;

    void serverSend(const std::string *source, int tag) override;

    // serverReceive
    void serverReceive(int64_t *target, int tag) override;

    void serverReceive(bool *target, int tag) override;

    void serverReceive(std::string *target, int tag) override;

    // send
    void send(const int64_t *source, int receiverRank, int tag) override;

    void send(const bool *source, int receiverRank, int tag) override;

    void send(const std::string *source, int receiverRank, int tag) override;

    //receive
    void receive(int64_t *target, int senderRank, int tag) override;

    void receive(bool *target, int senderRank, int tag) override;

    void receive(std::string *target, int senderRank, int tag) override;
};


#endif //MPC_PACKAGE_COMM_H
