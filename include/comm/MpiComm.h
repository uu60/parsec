//
// Created by 杜建璋 on 2024/7/15.
//

#ifndef MPC_PACKAGE_COMM_H
#define MPC_PACKAGE_COMM_H
#include "./Comm.h"
#include "item/MpiRequestWrapper.h"

/**
 * For sender, mpi rank must be 0 or 1.
 * The task publisher must be rank of 2.
 * Attention: Currently, there is no restriction in this util class.
 */
class MpiComm : public Comm {
public:
    static const int CLIENT_RANK;

private:
    // joined party number
    int _mpiSize{};
    // _mpiRank of current device
    int _mpiRank{};

public:
    int rank_() override;

    // init env
    void init_(int argc, char **argv) override;

    void finalize_() override;

    // judge identity
    bool isServer_() override;

    bool isClient_() override;

    void send_(int64_t source, int width, int receiverRank, int tag) override;

    // send
    void send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) override;

    void send_(const std::string &source, int receiverRank, int tag) override;

    void receive_(int64_t &source, int width, int senderRank, int tag) override;

    //receive
    void receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) override;

    void receive_(std::string &target, int senderRank, int tag) override;

    // async version
    MpiRequestWrapper *sendAsync_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) override;

    // send
    MpiRequestWrapper *sendAsync_(int64_t source, int width, int receiverRank, int tag) override;

    MpiRequestWrapper *sendAsync_(const std::string &source, int receiverRank, int tag) override;
    
    MpiRequestWrapper *receiveAsync_(int64_t &target, int width, int senderRank, int tag) override;
    
    MpiRequestWrapper *receiveAsync_(std::vector<int64_t> &target, int count, int width, int senderRank, int tag) override;
    
    MpiRequestWrapper *receiveAsync_(std::string &target, int length, int senderRank, int tag) override;
};


#endif //MPC_PACKAGE_COMM_H
